# splunkframeworkextension

OpenTelemetry Collector extension that boots the Splunk C++ framework and exposes it to other components as a pure-Go interface. **This is the only component in the pipeline that contains CGo or knows about Splunk C headers.**

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  splunk-col (single binary)                                  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  splunkframeworkextension               [CGo here]   │   │
│  │  • splunkfw_init / shutdown                          │   │
│  │  • splunk_pipeline_create / start / send / stop /   │   │
│  │    destroy  (conf-text-driven, covers inputs +       │   │
│  │    outputs; no new wrapper needed per component)     │   │
│  │  • exports splunkapi.SplunkFramework interface       │   │
│  └────────────────────┬─────────────────┬──────────────┘   │
│                        │ cfg.Framework   │                   │
│  ┌─────────────────────▼───┐  ┌─────────▼─────────────┐    │
│  │  splunktailreceiver      │  │  splunktcpoutexporter  │    │
│  │  [pure Go, no CGo]       │  │  [pure Go, no CGo]    │    │
│  │  builds InputsConf str   │  │  builds OutputsConf   │    │
│  │  fw.NewPipeline(...)     │  │  fw.NewPipeline(...)  │    │
│  └──────────────────────────┘  └───────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

The receiver and exporter obtain the extension at startup by a **direct component-ID lookup** — no iteration over all extensions:

```go
// In Config:
Framework component.ID `mapstructure:"framework"` // e.g. "splunkframework"

// In Start():
ext, ok := host.GetExtensions()[cfg.Framework]
fw := ext.(splunkapi.SplunkFramework)
p, err := fw.NewPipeline(splunkapi.PipelineConfig{
    InputsConf: "[monitor:///var/log/*.log]\nsourcetype=myapp\n",
})
```

No C headers, no linker flags, no CGo are needed in the receiver or exporter modules.

## Package layout

```
splunkframeworkextension/
  extension.go              # Linux CGo implementation; Start/Shutdown + NewPipeline
  extension_others.go       # Non-Linux stub
  config.go                 # Config struct (splunk_home, splunk_db)
  factory.go                # component.NewFactory registration
  splunkfw_cabi.h           # Vendored: Splunk framework bootstrap API
  splunk_pipeline_cabi.h    # Vendored: generic conf-driven pipeline CABI
  splunkapi/
    api.go                  # Pure-Go interfaces (SplunkFramework, Pipeline, ...)
```

### splunkapi interfaces

| Interface | Methods |
|---|---|
| `SplunkFramework` | `NewPipeline(PipelineConfig) (Pipeline, error)` |
| `Pipeline` | `Start() error` · `Events() <-chan Event` · `Send(body, source, sourcetype, host, index) error` · `Stop(drainSecs int)` · `Destroy()` |

`PipelineConfig` carries raw Splunk `.conf` stanza text:

| Field | Purpose |
|---|---|
| `InputsConf` | `inputs.conf` stanzas — `[monitor://glob]`, `[default]` |
| `OutputsConf` | `outputs.conf` stanzas — `[tcpout]`, `[tcpout:group]` |
| `PropsConf` | `props.conf` stanzas (Phase 2, currently unused) |

## Building

### Why `libsplunk_cabi.so` is necessary

The Splunk framework cannot be used by simply linking against `splunkd` or the libraries under `splunk_home/lib/`. There are two independent blockers:

**1. `splunkd` is an executable, not a library.**
`splunkd` has its own `main()` and is an ELF PIE executable. The linker will not accept it as `-lsplunkd`. While it is technically possible to `dlopen()` it at runtime, initialising it would start the entire Splunk daemon — REST server, cluster management, indexing threads — not just the tailing or forwarding subsystem.

**2. The internal C++ class headers are not shipped.**
`splunk_home/include/` contains only third-party public headers (Boost, Arrow, Abseil, libcares, etc.). The internal Splunk classes the CABI wrappers call — `TailReader`, `TcpOutputProcessorImpl`, `PropertyPages::saveStanza()`, `IProcessor` — are declared only in `main/src/`. Without those headers, `tailin_cabi.cpp` and `tcpout_cabi.cpp` cannot be compiled at all.

The dependency on `main/src/` is therefore **compile-time** (for C++ class declarations), not just link-time.

### How `libsplunk_cabi.so` is built

Each Splunk subsystem that is ported requires a plain-C wrapper pair:

```
main/src/framework_cabi/
  splunkfw_cabi.h / (built into splunkd's object tree)    # framework init/shutdown
  tailin_cabi.h   / tailin_cabi.cpp                       # file-tail input CABI
  tcpout_cabi.h   / tcpout_cabi.cpp                       # S2S TCP output CABI
  splunk_pipeline_cabi.h / splunk_pipeline_cabi.cpp        # conf-text generic router
```

The `*_cabi.h` files define a **plain-C ABI** (no C++ types, no name mangling) so CGo can call them directly. The `*_cabi.cpp` files include internal Splunk headers from `main/src/` and call the real C++ classes.

`splunk_pipeline_cabi.cpp` is a conf-text router: it parses raw `.conf` stanza text and delegates to `tailin_cabi` or `tcpout_cabi` internally. No new Go interface change is needed when adding a new stanza type — only a new branch in the router.

All four `.o` files are merged into one shared library:

```
splunkfw_cabi.o         ─┐
tailin_cabi.o           ─┤── libsplunk_cabi.so
tcpout_cabi.o           ─┤
splunk_pipeline_cabi.o  ─┘
```

Build it with:

```sh
cd main/src/framework_cabi && make -j$(nproc)
# produces: main/src/framework_cabi/libsplunk_cabi.so
```

### Adding a new Splunk component

1. Write `foo_cabi.cpp` — call the internal Splunk C++ factory/pipeline APIs (requires `main/src/` headers)
2. Write `foo_cabi.h` — plain-C header, the stable ABI boundary (no C++ types)
3. Add `foo_cabi.o` to the `Makefile` `SRCS` list, rebuild `.so`
4. Add a stanza-type branch in `splunk_pipeline_cabi.cpp` to route `[foo://...]` stanzas
5. No changes needed in Go code — callers pass raw conf text via `PipelineConfig`

### Building the OTel extension

Only `CGO_LDFLAGS` is required — both C headers (`splunkfw_cabi.h`, `splunk_pipeline_cabi.h`) are vendored in this directory and found automatically via `#cgo CFLAGS: -I${SRCDIR}`.

```sh
FW_DIR=/path/to/main/src/framework_cabi
SPLUNK_HOME=/path/to/splunk_home

CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi \
             -Wl,-rpath,${FW_DIR} \
             -Wl,-rpath,${SPLUNK_HOME}/lib \
             -lstdc++ -ldl -lpthread" \
go build ./...
```

## Runtime dependencies

All loaded by the single `splunk-col` binary via `LD_LIBRARY_PATH`:

| Library | Source |
|---|---|
| `libsplunk_cabi.so` | `main/src/framework_cabi/` — unified Splunk CABI |
| `libssl.so.3` | `splunk_home/lib/` — Splunk-bundled OpenSSL |
| `libcrypto.so.3` | `splunk_home/lib/` |
| `libz.so.1` | `splunk_home/lib/` |
| `libxml2.so` | `splunk_home/lib/` |
| `libpcre2-8.so.0` | `splunk_home/lib/` |
| `libarchive.so.13` | `splunk_home/lib/` |
| `libsqlite3.so.0` | `splunk_home/lib/` |
| `libcurl.so.4` | `splunk_home/lib/` |
| `libc.so.6` | system |
| `libstdc++.so.6` | system |
| `libm.so.6` | system |
| `libgcc_s.so.1` | system |
| `libatomic.so.1` | system |
| `libbz2.so.1.0` | system |
| `libdl.so.2` | system |
| `libpthread.so.0` | system |
| `libcares.so.2` | system |

Run with:

```sh
LD_LIBRARY_PATH=/path/to/main/src/framework_cabi:/path/to/splunk_home/lib \
SPLUNK_HOME=/path/to/splunk_home \
./splunk-col --config config.yaml
```

## Configuration

```yaml
extensions:
  splunkframework:
    splunk_home: /opt/splunk   # required
    splunk_db: /opt/splunk/var/lib/splunk  # optional

receivers:
  splunktail:
    framework: splunkframework  # component ID of the extension above
    monitors:
      - glob: /var/log/myapp/*.log
        sourcetype: myapp

exporters:
  splunktcpout:
    framework: splunkframework  # component ID of the extension above
    host: 10.0.0.1
    port: 9997

service:
  extensions: [splunkframework]
  pipelines:
    logs:
      receivers: [splunktail]
      exporters: [splunktcpout]
```

The extension **must** appear in `service.extensions` so `splunkfw_init()` completes before `Start()` is called on downstream components.
