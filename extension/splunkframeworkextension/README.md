# splunkframeworkextension

OpenTelemetry Collector extension that boots the Splunk C++ framework and exposes it to other components as a pure-Go interface. **This is the only component in the pipeline that contains CGo or knows about Splunk C headers.**

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  splunk-col (single binary)                             │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  splunkframeworkextension          [CGo here]   │   │
│  │  • splunkfw_init / shutdown                     │   │
│  │  • tailin_create / add_monitor / start / stop   │   │
│  │  • tcpout_create / send / destroy               │   │
│  │  • exports splunkapi.SplunkFramework interface  │   │
│  └──────────────┬──────────────────────┬───────────┘   │
│                 │ host.GetExtensions()  │               │
│  ┌──────────────▼──────┐  ┌────────────▼────────────┐  │
│  │  splunktailreceiver  │  │  splunktcpoutexporter   │  │
│  │  [pure Go, no CGo]  │  │  [pure Go, no CGo]      │  │
│  │  calls TailInput     │  │  calls TcpOutput        │  │
│  └──────────────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

The receiver and exporter obtain the extension at startup by iterating `host.GetExtensions()` and type-asserting to `splunkapi.SplunkFramework`:

```go
var fw splunkapi.SplunkFramework
for _, ext := range host.GetExtensions() {
    if f, ok := ext.(splunkapi.SplunkFramework); ok {
        fw = f; break
    }
}
ti, err := fw.NewTailInput(splunkapi.TailConfig{...})
out, err := fw.NewTcpOutput(host, port, index)
```

No C headers, no linker flags, no CGo are needed in the receiver or exporter modules.

## Package layout

```
splunkframeworkextension/
  extension.go          # Linux CGo implementation; Start/Shutdown + SplunkFramework impl
  extension_others.go   # Non-Linux stub
  config.go             # Config struct (splunk_home, splunk_db)
  factory.go            # component.NewFactory registration
  splunkfw_cabi.h       # Vendored: Splunk framework bootstrap API
  tailin_cabi.h         # Vendored: Splunk tail-input CABI
  tcpout_cabi.h         # Vendored: Splunk TCP-output CABI
  splunkapi/
    api.go              # Pure-Go interfaces (SplunkFramework, TailInput, TcpOutput, ...)
```

### splunkapi interfaces

| Interface | Methods |
|---|---|
| `SplunkFramework` | `NewTailInput(TailConfig) (TailInput, error)` · `NewTcpOutput(host, port, index) (TcpOutput, error)` |
| `TailInput` | `AddMonitor(MonitorConfig) error` · `Start() error` · `Events() <-chan TailEvent` · `Stop()` · `Destroy()` |
| `TcpOutput` | `Send(body, source, sourcetype, host, index) error` · `Destroy(drainSecs)` |

## Building

Only `CGO_LDFLAGS` is required — all three C headers are vendored in this directory and resolved automatically via `#cgo CFLAGS: -I${SRCDIR}`.

```sh
FW_DIR=/path/to/main/src/framework_cabi
SPLUNK_HOME=/path/to/splunk_home

CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi \
             -Wl,-rpath,${FW_DIR} \
             -Wl,-rpath,${SPLUNK_HOME}/lib \
             -lstdc++ -ldl -lpthread" \
go build ./...
```

Build `libsplunk_cabi.so` first (contains `splunkfw_cabi.o + tailin_cabi.o + tcpout_cabi.o`):

```sh
cd main/src/framework_cabi && make -j$(nproc)
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

## service.extensions requirement

The extension **must** appear in `service.extensions` before any receiver or exporter that uses it, so `splunkfw_init()` completes before `Start()` is called on downstream components:

```yaml
service:
  extensions: [splunkframework]
  pipelines:
    logs:
      receivers: [splunktail]
      exporters: [splunktcpout]
```
