# splunktcpoutexporter — Splunk S2S TCP Output Exporter

An OpenTelemetry Collector exporter that forwards log records to a Splunk indexer
over the native **S2S (cooked-stream) protocol**, using `libtcpout_cabi.so` — the
standalone C-ABI shared library extracted from Splunk's `tcpout` subsystem.

> **Platform:** Linux only (CGo + `libtcpout_cabi.so` required).

---

## Architecture

```
mock.log (tailed by filelog receiver)
        │  plog.Logs — each line = one log record
        │  attributes: splunk.sourcetype, splunk.source
        │  resource:   host.name
        ▼
OTel Collector pipeline
        ▼
splunktcpoutexporter  (exporter.go)
        │  CGo call per log record
        ▼
libtcpout_cabi.so     (tcpout_cabi.h / tcpout_cabi.cpp)
  extern "C" boundary
        │  statically linked
        ▼
libtcpout.a + libsupport.a
  → S2S cooked-stream over TCP → Splunk indexer :9997  index=main
```

---

## Prerequisites

### 1. Build `libtcpout_cabi.so`

```bash
cd /home/chli/main/src/output/tcpout_lib

# First-time: build ~950 framework objects (parallelise; takes a few minutes)
make -j8 libsupport.a

# Build the 17 tcpout objects
make lib

# Build the C-ABI shared library (libtcpout_cabi.so ~1.1 GB)
make cabi
```

### 2. One-time `libbz2` symlink fix

`libarchive.so` in `splunk_home/lib` links against `libbz2` but only `libbz2.so.1`
exists on the system (no `-dev` package installed):

```bash
ln -s /usr/lib/x86_64-linux-gnu/libbz2.so.1 /home/chli/splunk_home/lib/libbz2.so
```

---

## Build the exporter package

```bash
export TCPOUT_LIB=/home/chli/main/src/output/tcpout_lib
export SPLUNK_HOME_LIB=/home/chli/splunk_home/lib

cd /home/chli/otel/opentelemetry-collector-contrib/exporter/splunktcpoutexporter

CGO_CFLAGS="-I${TCPOUT_LIB}" \
CGO_LDFLAGS="-L${TCPOUT_LIB} -L${SPLUNK_HOME_LIB} \
             -Wl,-rpath,${TCPOUT_LIB} \
             -Wl,-rpath,${SPLUNK_HOME_LIB}" \
go build ./...
```

To verify without linking an executable:

```bash
CGO_CFLAGS="-I${TCPOUT_LIB}" \
CGO_LDFLAGS="-L${TCPOUT_LIB} -L${SPLUNK_HOME_LIB} \
             -Wl,-rpath,${TCPOUT_LIB} \
             -Wl,-rpath,${SPLUNK_HOME_LIB}" \
go vet ./...
```

---

## Demo: filelog receiver → splunktcpout exporter

The `demo/` directory contains everything needed to run an end-to-end pipeline that
tails a mock log file and forwards each line to a Splunk indexer via S2S.

```
demo/
├── builder-config.yaml   OTel Collector Builder (ocb) manifest
├── config.yaml           Collector runtime config (filelog → splunktcpout + debug)
├── mock.log              10 pre-seeded log lines (filelog tails this at startup)
├── gen_mock_log.sh       Appends synthetic events to mock.log (run in a 2nd terminal)
└── build.sh              Installs ocb if absent, sets CGo flags, builds the binary
```

### Step 1 — Build the custom collector binary

`build.sh` installs `ocb` (OpenTelemetry Collector Builder) if not already present,
then compiles a minimal binary containing only the filelog receiver and the
splunktcpout exporter (plus a debug exporter for console visibility).

```bash
cd /home/chli/otel/opentelemetry-collector-contrib/exporter/splunktcpoutexporter/demo
./build.sh
# → outputs bin/splunktcpout-demo-col
```

### Step 2 — (Optional) stream new events into mock.log

In a second terminal, run the generator to keep appending events while the collector
is running:

```bash
# append one event per second indefinitely
./gen_mock_log.sh

# or: stop after 20 events, 200 ms between each
./gen_mock_log.sh 20 200
```

### Step 3 — Run the collector

```bash
LD_LIBRARY_PATH=/home/chli/main/src/output/tcpout_lib:/home/chli/splunk_home/lib \
SPLUNK_HOME=/home/chli/splunk_home \
./bin/splunktcpout-demo-col --config config.yaml
```

The `debug` exporter prints every record to stdout so you can confirm events are
flowing before checking Splunk.

### What the pipeline does

| Stage | Detail |
|-------|--------|
| **filelog receiver** | Tails `demo/mock.log`; each line becomes one `plog.LogRecord` |
| **attributes** | `splunk.sourcetype=filelog_demo`, `splunk.source=demo/mock.log` set on all records |
| **resource** | `host.name=demo-otel-host` set on the resource scope |
| **splunktcpout exporter** | Calls `tcpout_send()` per record; maps OTel attributes → S2S fields |
| **Splunk indexer** | Receives events at `10.236.40.125:9997`, stored in index `main` |

---

## Configuration reference

See [`config.yaml`](config.yaml) for a ready-to-use example. All fields:

```yaml
exporters:
  splunktcpout:
    host: 10.236.40.125        # Splunk indexer hostname / IP  (required)
    port: 9997                 # S2S receive port              (default: 9997)
    index: main                # Default Splunk index          (optional)
    default_source: otel       # source field fallback
    default_sourcetype: otel   # sourcetype field fallback     (default: "otel")
    default_host: ""           # host field fallback           (empty = local hostname)
    drain_seconds: 5           # Queue drain wait on shutdown  (default: 5)
    splunk_home: /home/chli/splunk_home   # Sets SPLUNK_HOME before C init
```

### Log-record attribute → Splunk field mapping

| OTel attribute (log record) | OTel attribute (resource) | Splunk field | Fallback |
|-----------------------------|---------------------------|--------------|----------|
| `splunk.source` | — | `source` | `default_source` |
| `splunk.sourcetype` | — | `sourcetype` | `default_sourcetype` |
| `host.name` | `host.name` | `host` | `default_host` |
| `splunk.index` | — | `index` | `index` config field |

Record-level attributes take priority over resource-level attributes, which take
priority over the config-file defaults.

---

## Runtime environment

```bash
export SPLUNK_HOME=/home/chli/splunk_home
export LD_LIBRARY_PATH=/home/chli/main/src/output/tcpout_lib:/home/chli/splunk_home/lib
```

`SPLUNK_HOME` must point to a directory containing an `etc/` sub-tree from which
the Splunk framework reads `outputs.conf` and `server.conf`.  It can also be set
via the `splunk_home:` field in the exporter config.

---

## Benchmark results

### Column definitions

| Column | Binary | Framework |
|--------|--------|-----------|
| `C++ tcpout_sender` | standalone C++ sender | `EmptyMainThread` stub |
| `Go CGo go_tcpout` | standalone Go CGo sender | `EmptyMainThread` stub |
| `OTel e2e` | filelog → splunktcpout (no extension) | `EmptyMainThread` stub |
| `OTel + framework` | filelog → splunktcpout + `splunkframeworkextension` | full `SplunkMainThread` |

Throughput for the first three columns measured via `/proc/net/dev` TX counters (all
interface TX).  The "OTel + framework" column uses **`tcpdump`** capturing only TCP
traffic to port 9997, which is the more accurate method — it counts actual bytes
delivered to the indexer and ignores unrelated interface traffic.  `tcpout` config:
`maxQueueSize=512 KB`, `maxConnectionsPerIndexer=2` (minimal/test settings).

### Results

| Payload | Metric | C++ `tcpout_sender` | Go CGo `go_tcpout` | OTel e2e | OTel + framework<br>(queue=512KB, conn=2) | OTel + framework<br>(queue=8MB, conn=4) |
|---------|--------|:-------------------:|:-----------------:|:--------:|:-----------------------------------------:|:---------------------------------------:|
| **100 000 × 1 KB** | Wire throughput | 15.7 MB/s | 24.5 MB/s | 31.5 MB/s | 11.9 MB/s | **31.9 MB/s** |
| | Active window | 7 440 ms | 4 969 ms | 2 534 ms | 8 766 ms | **2 593 ms** |
| | CPU time | 8.9 s | 2.1 s | 2.3 s | — | **3.2 s** |
| | Peak RSS | **45 MB** | 56 MB | 80 MB | 89 MB | 101 MB |
| | Peak PSS | — | — | — | 85 MB | **97 MB** |
| **1 000 × 1 MB** | Wire throughput | 24.9 MB/s | 24.8 MB/s | 28.2 MB/s | 20.4 MB/s | **20.3 MB/s** |
| | Active window | 41 359 ms | 41 396 ms | 34 143 ms | 49 018 ms | **48 823 ms** |
| | CPU time | 26.2 s | 3.0 s | 3.0 s | — | **2.4 s** |
| | Peak RSS | **48 MB** | 83 MB | 485 MB | 474 MB | **518 MB** |
| | Peak PSS | — | — | — | 471 MB | **514 MB** |

> **Host:** `ufcompo` (Linux, 32 vCPU) · **Date (first three cols):** 2026-05-02 · **Date (OTel+framework cols):** 2026-05-08

> **PSS vs RSS:** RSS counts shared-library pages in full per process; PSS
> (Proportional Set Size) divides shared pages by the number of processes sharing them.
> Since `libsplunk_cabi.so` is loaded by exactly one process, PSS ≈ RSS — the ~4 MB
> gap is Go runtime and libc pages shared with the shell ancestor.  PSS is reported
> because it becomes meaningful when multiple collector instances share the same `.so`.

### Why the throughput ordering differs

**1 — Go CGo > C++ on 1 KB: blocking call vs spin-wait**

`tcpout_send()` (CGo) blocks on a condition variable (futex) when the 512 KB queue is
full — the OS thread sleeps, the goroutine parks, zero CPU consumed.  C++'s `sendData()`
spin-waits in a tight loop with short `usleep` calls, burning a CPU core continuously.
At 1 KB/event (~37 µs per enqueue), spin iterations are shorter than the OS scheduler
time-slice (~10 ms), so the spinning main thread starves the two background sender threads
that need CPU to drain the queue.  Measured cost: **6.8 s wasted CPU → 8.8 MB/s of lost
throughput** (15.7 → 24.5 MB/s).

**2 — OTel > Go CGo on 1 KB: pipeline read-ahead, no batch processor**

The filelog receiver goroutine runs independently of the exporter goroutine.  While the
exporter is blocked inside `tcpout_send()` on record N, the receiver is already reading
and parsing records N+1, N+2, … into the pipeline buffer.  When `tcpout_send()` returns,
the next record is ready immediately — no file-I/O stall.  `go_tcpout` is a single
goroutine serialising read → send → read → send with a Go scheduler checkpoint between
each pair, leaving the queue momentarily under-filled.  OTel keeps the queue more
continuously saturated → **31.5 MB/s vs 24.5 MB/s**.

**3 — All three converge at 1 MB: queue capacity dominates**

A 1 MB event is 2× the 512 KB queue capacity.  Every send call must wait ~16 ms for the
background threads to drain the queue before the next event can begin enqueuing.  That
16 ms wait is longer than a scheduler time-slice, so sender threads get uncontested CPU
regardless of spin-wait behaviour (reason 1 no longer applies) and the inter-call
scheduler gap is negligible relative to 16 ms (reason 2 no longer applies).  All three
implementations are gated by the same TCP send rate → ~25 MB/s.  OTel's read-ahead
still contributes a modest 13% edge (28.2 MB/s) by overlapping parse of the next record
with the current send.  The main surviving difference is **RSS**: OTel buffers multiple
1 MB records in Go heap simultaneously → 485 MB; Go CGo holds one at a time → 83 MB;
C++ has no Go heap → 48 MB.

**4 — OTel + framework throughput regression: SplunkMainThread CPU contention**

The "OTel + framework" column is **2.6× slower at 1 KB** (11.9 vs 31.5 MB/s) and
**28% slower at 1 MB** (20.4 vs 28.2 MB/s).  The root cause is CPU contention between
the framework's background threads and the tcpout sender threads.

`splunkframeworkextension` starts a full `SplunkMainThread` with `forgoProcessRunnerInit=true`
(skipping the process lifecycle manager), but still launches the complete internal
machinery:

| Thread group | Count | Purpose |
|---|---|---|
| EventLoop (epoll) | 1 | Conf-change, signal, heartbeat dispatch |
| Pipeline stage queues | 3–4 | Parsing, typing, indexing queues (all idle but scheduled) |
| FileTracker / inotify | 2 | Watches all configured `monitor://` stanzas |
| BulletinBoardManager | 1 | In-memory conf change propagation |
| Health reporter | 1 | Periodic metrics collection |
| `SplunkMainThread` itself | 1 | Orchestration loop |

At 1 KB/event, `tcpout_send()` completes in ~37 µs.  The sender threads re-enter the
OS scheduler frequently enough that the ~10 framework threads compete for scheduler
time-slices on every drain cycle.  At 1 MB/event the 16 ms drain wait is far larger
than a scheduler slice — framework threads get scheduled opportunistically and the
relative cost drops to 28%.

**Improvement plan**

Three levers in rough implementation order:

1. **Increase tcpout queue + connections (tested, fully recovers 1KB throughput)**
   Raising `maxQueueSize` from 512 KB to 8 MB and `maxConnectionsPerIndexer` from 2
   to 4 in the C++ init path (`tcpout_cabi.cpp`) fully recovers the 1KB throughput:
   **11.9 → 31.9 MB/s** (+168%), matching and marginally exceeding the OTel e2e
   baseline.  The larger queue lets the 4 sender threads stay continuously saturated,
   eliminating queue-full stalls as the contention window.  The 1MB case is unchanged
   (**20.3 MB/s**) — it is bottlenecked purely on TCP bandwidth and the per-send drain
   time is independent of queue size.  RSS increases from 89 MB to 101 MB at 1KB
   (the 8MB queue is pre-allocated in C++ heap).  This is now the default in
   `tcpout_cabi.cpp`.

2. **CPU-set isolation (deployment, no code change)**
   Pin the OTel process to a subset of CPUs away from framework-heavy cores:
   ```bash
   taskset -c 0-15 ./splunk-col --config config.yaml
   ```
   On a 32-vCPU host this reserves 16 cores for tcpout sender threads and Go
   goroutines.  On a production host with dedicated vCPUs this is the zero-cost fix.

3. **Reduce framework poll aggressiveness in extension mode (code change)**
   `EventLoop::enableFastPoll()` is called unconditionally in `splunkfw_cabi.cpp`.
   In extension mode there are no real inputs to process, so fast poll just burns
   CPU.  Replacing it with a slower poll interval (e.g., `setDefaultPollInterval(100)`
   instead of fast mode) should reduce framework thread wake-ups by ~10×.  This
   requires a small change to `splunkfw_cabi.cpp` and re-linking `libsplunk_cabi.so`.


