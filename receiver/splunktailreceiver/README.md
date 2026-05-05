# splunktailreceiver

An OpenTelemetry Collector receiver that reads log files using the Splunk tail
input library (`libtailinput_cabi.so`). Linux only (CGo).

---

## Architecture

### Two-process pipeline

The two CGo libraries share global Splunk framework singletons and **cannot
coexist in one process**:

| Library | Bootstrap | Why it conflicts |
|---|---|---|
| `libtailinput_cabi.so` | `SplunkMainThread` (full EventLoop, singleton `_instance`) | `TailManager` uses `ScopedJoinAndDelete` → `SplunkMainThread::runInThreadNowait` → dereferences `_instance` unconditionally |
| `libtcpout_cabi.so` | `EmptyMainThread` (no EventLoop) | `MainThread()` constructor **throws** if a `MainThread` already exists |

Because of this, the full tail→index pipeline runs as **two separate collector
binaries** bridged by OTLP HTTP:

```
┌─────────────────────────────┐        OTLP HTTP        ┌──────────────────────────────────┐
│  splunktail-col  (Binary A) │ ───── localhost:4318 ──► │  splunktcpout-otlp-col (Binary B) │
│                             │                          │                                  │
│  splunktailreceiver         │                          │  otlpreceiver                    │
│    └─ libtailinput_cabi.so  │                          │  splunktcpoutexporter            │
│         SplunkMainThread    │                          │    └─ libtcpout_cabi.so          │
│                             │                          │         EmptyMainThread          │
└─────────────────────────────┘                          └──────────────────────────────────┘
                                                                        │
                                                                        ▼
                                                             Splunk indexer (S2S TCP)
                                                             e.g. 10.236.40.125:9997
```

For the design rationale, component hierarchy, path to single-process architecture,
and the splunkd-as-sidecar alternative, see [shared_extensions.md](shared_extensions.md).

### Why extra files get ingested (inputs.conf)

`tailin_create()` calls `LoaderInfo::instance()->populateFromEnvironment()`,
which causes the Splunk framework to load **all inputs** from:

```
$SPLUNK_HOME/etc/system/default/inputs.conf   ← 13 built-in stanzas
$SPLUNK_HOME/etc/system/local/inputs.conf
$SPLUNK_HOME/etc/apps/*/default/inputs.conf
```

The built-in `[monitor://$SPLUNK_HOME/etc/splunk.version]` stanza (line 39 of
`default/inputs.conf`) is one example. The `TailManager` monitors every stanza
it finds, not just the user-configured globs.

Events from these built-in inputs arrive at `goTailinEventCallback` with their
own sourcetypes (`splunk_version`, etc.). The callback currently forwards
everything — filter by sourcetype in the callback if you only want user-defined
monitors.

---

## Building

Both binaries are built from generated source under `demo/`:

```bash
DEMO=/path/to/receiver/splunktailreceiver/demo

# Generate sources (skip compilation)
GOPATH=/home/chli/go builder --config $DEMO/builder-config-A.yaml --skip-compilation=true
GOPATH=/home/chli/go builder --config $DEMO/builder-config-B.yaml --skip-compilation=true

# Build Binary A (splunktailreceiver + otlphttpexporter)
cd $DEMO/binA
CGO_CFLAGS="-I/home/chli/main/src/input/tail_lib" \
CGO_LDFLAGS="-L/home/chli/main/src/input/tail_lib -L/home/chli/splunk_home/lib \
             -Wl,-rpath,/home/chli/main/src/input/tail_lib \
             -Wl,-rpath,/home/chli/splunk_home/lib" \
GOPATH=/home/chli/go \
go build -trimpath -o splunktail-col -ldflags="-s -w" .

# Build Binary B (otlpreceiver + splunktcpoutexporter)
cd $DEMO/binB
CGO_CFLAGS="-I/home/chli/main/src/output/tcpout_lib" \
CGO_LDFLAGS="-L/home/chli/main/src/output/tcpout_lib -L/home/chli/splunk_home/lib \
             -Wl,-rpath,/home/chli/main/src/output/tcpout_lib \
             -Wl,-rpath,/home/chli/splunk_home/lib" \
GOPATH=/home/chli/go \
go build -trimpath -o splunktcpout-otlp-col -ldflags="-s -w" .
```

The `builder` binary is at `/home/chli/go/bin/builder` (ocb v0.151.0).

---

## Running (E2E test)

```bash
DEMO=/path/to/receiver/splunktailreceiver/demo

# 1. Start Binary B first (OTLP receiver → Splunk indexer)
LD_LIBRARY_PATH=/home/chli/main/src/output/tcpout_lib:/home/chli/splunk_home/lib \
SPLUNK_HOME=/home/chli/splunk_home \
$DEMO/binB/splunktcpout-otlp-col --config $DEMO/config-B.yaml &

# 2. Start Binary A (tail reader → OTLP)
LD_LIBRARY_PATH=/home/chli/main/src/input/tail_lib:/home/chli/splunk_home/lib \
SPLUNK_HOME=/home/chli/splunk_home \
$DEMO/binA/splunktail-col --config $DEMO/config-A.yaml
```

Verify in Splunk:
```
index=main sourcetype=tailin_e2e | stats count
```

---

## Key source files

| File | Purpose |
|---|---|
| `receiver.go` | Linux CGo receiver implementation, `goTailinEventCallback` trampoline |
| `receiver_unsupported.go` | Stub for non-Linux platforms |
| `factory.go` | `receiver.Factory` registration |
| `config.go` | `Config` struct with `Monitors`, `SplunkHome`, etc. |
| `tailin_cabi.h` | Vendored C header from `main/src/input/tail_lib/` |
| `demo/config-A.yaml` | Binary A runtime config |
| `demo/config-B.yaml` | Binary B runtime config |
| `demo/builder-config-A.yaml` | ocb builder config for Binary A |
| `demo/builder-config-B.yaml` | ocb builder config for Binary B |
