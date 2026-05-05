# Splunk Framework — Shared OTel Extensions Design

This document covers the architecture of porting Splunk's internal C++ framework
subsystems to OpenTelemetry Collector **extensions**, allowing multiple Splunk-backed
receivers and exporters to share a single instance of each subsystem in one process.

---

## Background — Why two binaries today

The two CGo libraries share global Splunk framework singletons and **cannot coexist
in one process** without code changes:

| Library | Bootstrap | Conflict |
|---|---|---|
| `libtailinput_cabi.so` | `SplunkMainThread` (full EventLoop, singleton `_instance`) | `TailManager` uses `ScopedJoinAndDelete` → `runInThreadNowait` → unconditional `_instance` deref |
| `libtcpout_cabi.so` | `EmptyMainThread` (no EventLoop) | `MainThread()` constructor **throws** if any `MainThread` already exists |

Both `.so` files export `SplunkMainThread::_instance` with default ELF visibility.
The Linux dynamic linker deduplicates default-visibility symbols — both libraries
share one `_instance`. After `tailin_create()` sets it, `tcpout_create()`'s
`MainThread()` call finds it already set and throws.

**Inside splunkd this never happens**: `splunkd main()` (`Loader.cpp`) creates
`SplunkMainThread` exactly once before loading any module. Both tailin and tcpout
register with the already-running framework. No second construction, no conflict.
The OTel extension design replicates this.

---

## Three options for a single-process pipeline

The immediate goal is to connect `splunktailreceiver` and `splunktcpoutexporter`
inside **one OTel Collector process** — eliminating the two-binary OTLP bridge and
enabling direct validation that unparsed raw bytes (not pre-parsed strings) can flow
through an OTel pipeline end-to-end. Three approaches resolve the singleton conflict
at different levels of implementation effort.

### Option A — `-fvisibility=hidden` (self-contained libraries)

Add `-fvisibility=hidden -fvisibility-inlines-hidden` to the compiler flags for
both `.so` builds:

```makefile
# In tail_lib/Makefile and tcpout_lib/Makefile
CXXFLAGS += -fvisibility=hidden -fvisibility-inlines-hidden
```

With hidden visibility, `SplunkMainThread::_instance` is no longer a
default-visibility ELF symbol. The dynamic linker stops deduplicating it —
each `.so` gets its own private copy. `tailin_create()` sets its own `_instance`;
`tcpout_create()` sets its own separate `_instance`; no conflict.

Any symbols that must be visible across the `.so` boundary (the C ABI entry points)
need an explicit annotation:

```cpp
__attribute__((visibility("default"))) void tailin_create(...);
```

In practice this is only the `extern "C"` functions in each `_cabi.cpp` file —
a small surface.

**Performance implication:** each library still runs its own `EventLoop`. The
tailin `.so` runs a full `SplunkMainThread` EventLoop; the tcpout `.so` runs a
per-worker EventLoop. These are independent reactors in the same process —
redundant timer/poll cycles, extra threads, and duplicated in-memory state. No
serialization overhead (shared process memory), but more CPU and RSS than a
single shared EventLoop would require.

### Option B — OTel extension as a shared runtime resource

Port `SplunkMainThread` (and its owned threads) into a dedicated OTel
**extension** — `splunkframeworkextension`. By OTel design, extensions are
process-level singletons with an explicit `Start()`/`Shutdown()` lifecycle,
discoverable by other components via `host.GetExtensions()`. This mirrors exactly
how `Loader.cpp` works: one framework init, then modules attach.

Both `tailin_create()` and `tcpout_create()` are changed to detect an already-running
`SplunkMainThread` and skip their own bootstrap. The extension owns the single
`EventLoop`; both components share it.

Other shared splunkd subsystems (SSL, HTTP, conf management) can be added as
additional extensions in layers — see [Approach A — OTel Extensions](#approach-a--otel-extensions-incremental-porting) below.

**Performance implication:** one `EventLoop` drives all components — no redundant
reactors, no duplicated state. This is the most efficient single-process design.
The tradeoff is implementation complexity: each shared subsystem requires a new
extension with explicit lifecycle and ownership.

### Option C — Run splunkd as an OTel sidecar process

Run a real splunkd process alongside the OTel Collector and connect them via a
standard protocol (S2S, HEC, or OTLP). No CGo changes, no porting — the boundary
is a wire protocol.

**Performance implication:** every event crosses a process boundary — serialization,
a loopback TCP round-trip, and deserialization. Latency is sub-millisecond on
loopback but nonzero, and throughput is bounded by the IPC socket. Two separate
processes also means two copies of the Splunk framework in memory.

### Option D — Rewrite the C++ components in Go

Replace `libtailinput_cabi.so` and `libtcpout_cabi.so` with pure Go implementations,
eliminating CGo and the Splunk C++ framework entirely.

This faces the same structural problems as Options A and B:

- **Singleton / shared-state problem** — the Go implementations still need a shared
  process-level reactor (goroutine scheduler + channel infrastructure) and coordinated
  shutdown. The problem shifts from `SplunkMainThread` to designing equivalent
  Go-native shared resources — Options A and B are still the relevant design patterns,
  just expressed in Go instead of C++.
- **Feature parity** — the Splunk tail library implements decades of production-hardened
  behaviour: fishbucket (tail-position persistence across restarts), props/transforms
  pipeline (`LineBreaker`, `HeaderProcessing`, `QueueInputProcessor`), `TailManager`
  file-rotation and inode-tracking logic, and the full S2S wire protocol in tcpout.
  Reproducing this in Go is a large, long-running effort with significant risk of
  subtle behavioural divergence.

**Performance implication:** a pure Go implementation would have the lowest runtime
overhang (no CGo call overhead, Go GC, no C++ framework threads). But this benefit
is only realised after full feature parity is achieved — which is the hard part.

### Comparison

| | Performance overhead | Implementation complexity | OTel compatibility |
|---|---|---|---|
| **Option A** (`-fvisibility=hidden`) | **High** — duplicated `EventLoop` per library; extra threads, extra memory, redundant poll cycles | **Low** — build flag only; no C++ changes | **Medium** — each lib is isolated; no shared OTel lifecycle; awkward to coordinate shutdown |
| **Option B** (OTel extensions) | **Low** — single shared `EventLoop`; minimal overhead | **High** — must port each shared subsystem as a new extension with explicit ownership | **High** — native OTel model; extensions compose cleanly; pipelines share resources correctly |
| **Option C** (splunkd sidecar) | **Medium** — IPC serialization cost per event; two processes; two framework copies in memory | **Low** — no porting required; splunkd runs as-is | **Low** — protocol boundary only; OTel cannot introspect or compose Splunk internals |
| **Option D** (Go rewrite) | **Low** (once done) — no CGo overhead, no C++ framework threads | **Very High** — full feature parity (fishbucket, transforms, S2S protocol, rotation logic) is a large multi-year effort; same shared-state design questions as A/B still apply | **High** — pure Go, native OTel model, no CGo boundary |

**Recommended starting point:** Option B, Layer 0 only (`splunkframeworkextension`).
One extension, one `SplunkMainThread`, minimal C++ change to both cabi wrappers.
This unblocks the single-process pipeline with the lowest performance overhead and
a clean OTel-native ownership model.

---

## Splunk component hierarchy

| Component | What it is | Where it lives |
|---|---|---|
| **Launcher** (`splunk start`) | CLI tool (`launcher/main.cpp`); parses `start`/`stop`/`restart`; `exec()`s the splunkd binary as a new process | separate binary, exits after exec |
| **splunkd** (`Loader.cpp`) | The actual service process; performs all framework init in sequence before any modules run | `int main()` in splunkd |
| **`SplunkMainThread`** | A `Thread` that owns one `EventLoop` (`_eloop`); the framework's central reactor and thread-lifecycle manager; process-wide singleton (`_instance`) | constructed in `Loader.cpp`; runs `_eloop.run()` on its dedicated thread |
| **`EventLoop`** | Extends `TimeoutHeap`; adds `epoll`/`kqueue` I/O polling + cross-thread `InThreadActor` queue; must be driven by one thread calling `.run()` | member `_eloop` inside `SplunkMainThread` |
| **`TimeoutHeap`** | Pure timer priority-queue; base class of `EventLoop` | base class of `EventLoop` |
| **`CallbackRunnerThread`** | Spawned by `SplunkMainThread::main()`; runs `PeriodicCallback` registrations (log rolling, health, metrics) off the EventLoop thread | lives while `SplunkMainThread::main()` runs |
| **`ShutdownThread`** | Spawned by `SplunkMainThread::main()`; runs its own `EventLoop`; drives ordered multi-level shutdown (calls each `ShutdownHandler::signalShutdown()` level by level, then `SplunkMainThread::terminate()`) | lives while `SplunkMainThread::main()` runs |
| **`ProcessRunner`** | A **forked child process** + `ProcessRunnerThread`; receives `fork()` requests via Unix socket; ensures child-process spawning happens from a single-threaded context (safe `fork()`) | spawned by `ProcessRunnerInit()` inside `SplunkMainThread` ctor — **skipped by cabi wrappers** (`forgoProcessRunnerInit=true`) |
| **Module worker threads** (TailManager, RemoteQueueOutputWorker, …) | Spawned by modules via `SplunkMainThread::launchThreadAfterStartup()`; after launch they are independent | owned by each module |

**What `SplunkMainThread` is for** (why it exists):

1. **Thread lifecycle management** — `ScopedJoinAndDelete` posts `thr->join()` onto the
   EventLoop so all thread joins happen safely on one reactor thread. This is the
   primary reason tailin *needs* a running `SplunkMainThread`.
2. **Cross-thread message delivery** — `runInThreadNowait(functor)` lets any thread post
   work to the EventLoop thread safely.
3. **Periodic scheduling** — `_eloop` fires `PeriodicTimeout` callbacks (log rolling,
   health reporting). `CallbackRunnerThread` offloads heavier ones.
4. **Shutdown coordination** — `ShutdownThread` drives the multi-level shutdown sequence.

Splunkd startup order:

```
splunk start  (launcher — separate binary, exits after exec)
    └─ exec() →  splunkd (Loader.cpp)
                    ├─ LoaderInfo / Logger init
                    ├─ SslContext::globalInit()        ← OpenSSL  (not needed by cabi today)
                    ├─ HTTPServer bind ports           ← REST API (not needed by cabi today)
                    ├─ new SplunkMainThread()          ← ONE construction, process-wide
                    │     ├─ ProcessRunnerInit()       ← forks child process (skipped by cabi)
                    │     ├─ CallbackRunnerThread      ← spawned inside SplunkMainThread::main()
                    │     ├─ ShutdownThread            ← spawned inside SplunkMainThread::main()
                    │     └─ _eloop.run()              ← central EventLoop running
                    ├─ HTTPDispatchThread.go()         ← REST handler (not needed by cabi today)
                    └─ modules load as plugins         ← tailin, tcpout join; no re-bootstrap
```

---

## Approach A — OTel Extensions (incremental porting)

Each shared Splunk subsystem becomes its own OTel **extension** — a process-level
singleton with explicit `Start()`/`Shutdown()` lifecycle, discoverable by other
components via `host.GetExtensions()`. This mirrors exactly how `Loader.cpp` works:
it is the "all extensions combined into one sequential init function"; the extensions
decompose that into independently composable pieces.

### Layer 0 — `splunkframeworkextension` (required for all Splunk components)

Owns `SplunkMainThread` + core bootstrap. Must start first, stop last.

**What it initializes:**

| Step | Notes |
|---|---|
| `ScopedXmlLibInit` | libxml2 process init |
| `Logger::initialize()` | idempotent; guards double-init |
| `setenv(SPLUNK_HOME/DB/ETC)` | derives `SPLUNK_ETC` from `SPLUNK_HOME` if not set |
| `GlobalConfSettings::setInMemoryConfChanges()` | conf lives in-memory; no disk writes |
| `LoaderInfo::populateFromEnvironment()` | reads env vars; idempotent |
| `LoaderInfo::setTestMode()` | suppresses auth/DMC/OTel/IPC subsystems |
| `LoaderInfo::setDedFwder()` | marks process as Universal Forwarder (no indexer) |
| `BulletinBoardManager::instance()` | process-wide lazy singleton |
| `EventLoop::enableFastPoll()` | must happen before any `EventLoop` is created |
| `PluginProcessor::registerFactoryLookupFunction()` | plugin registry for tailin pipeline |
| `FileTracker::init()` | fishbucket (tail position persistence) |
| `new SplunkMainThread(forgoProcessRunnerInit=true)` | starts EventLoop on dedicated thread |

**C ABI (`splunkfw_cabi.cpp` — new file):**

```cpp
extern "C" void splunkfw_init(const char* splunk_home, const char* splunk_db) {
    static std::once_flag s_once;
    std::call_once(s_once, [&] {
        static ScopedXmlLibInit* s_xml = new ScopedXmlLibInit();
        Logger::initialize();
        setenv("SPLUNK_HOME", splunk_home, 1);
        setenv("SPLUNK_DB",   splunk_db,   0);
        if (!getenv("SPLUNK_ETC"))
            setenv("SPLUNK_ETC", (std::string(splunk_home)+"/etc").c_str(), 0);
        GlobalConfSettings::instance()->setInMemoryConfChanges();
        LoaderInfo::instance()->populateFromEnvironment();
        LoaderInfo::instance()->setTestMode();
        LoaderInfo::instance()->setDedFwder();
        BulletinBoardManager::instance();
        EventLoop::enableFastPoll();
        PluginProcessor::registerFactoryLookupFunction(getPluginFactoryByName);
        // fishbucket ...
        s_smtThread = std::thread([] {
            SplunkMainThread smt(Pathname::EMPTY_PATHNAME,
                                 /*forgoProcessRunnerInit=*/true,
                                 /*enableWatchdog=*/false);
            SplunkMainThread::RunActorAfterStartup raas(&s_startupSignal);
            smt.run();
        });
        // wait for EventLoop to be up ...
    });
}
extern "C" void splunkfw_shutdown() {
    SplunkMainThread::terminate(0);
    s_smtThread.join();
}
```

**Go wrapper:**

```go
// splunkframeworkextension/extension.go  //go:build linux
func (e *splunkFrameworkExt) Start(_ context.Context, _ component.Host) error {
    home := C.CString(e.config.SplunkHome)
    db   := C.CString(e.config.SplunkDb)
    defer C.free(unsafe.Pointer(home)); defer C.free(unsafe.Pointer(db))
    C.splunkfw_init(home, db)
    return nil
}
func (e *splunkFrameworkExt) Shutdown(_ context.Context) error {
    C.splunkfw_shutdown()
    return nil
}
```

**Required cabi changes to make both libs share the extension:**

- `tailin_cabi.cpp`: remove the `s_bootstrapped` bootstrap guard (Logger, LoaderInfo,
  SplunkMainThread thread). Assert `SplunkMainThread::isRunning()` at entry, then
  proceed directly to building `TailinHandle`.
- `tcpout_cabi.cpp`: guard `new EmptyMainThread()` — skip it when `SplunkMainThread`
  is already running:

```cpp
if (!SplunkMainThread::isRunning()) {
    static EmptyMainThread* s_mainThread = new EmptyMainThread();
    (void)s_mainThread;
}
```

### Layer 1 — `splunksslextension` (TLS connections)

Owns `SslContext::globalInit()` — the process-wide OpenSSL/FIPS init.

- **Depends on**: `splunkframeworkextension`
- **Needed by**: tcpout with SSL, any component making TLS connections, HTTP extension
- **C call**: `SslContext::globalInit()` once; no shutdown needed (static OpenSSL state)
- **Note**: cabi wrappers today skip this because `setTestMode()` suppresses auth paths
  that would enforce it

### Layer 2 — `splunkhttpextension` (management port + REST API)

Owns `HTTPServer` bind + `HTTPDispatchThread`.

- **Depends on**: `splunksslextension`
- **Exposes**: management port (default 8089); serves `/services/*` REST endpoints
- **Needed by**: any component that reads config via REST, conf management, search API
- **Key change**: must call `LoaderInfo::unsetTestMode()` so auth paths activate; this
  means `UserManager`, `LocalAccessToken`, `Duo2FA` all become live

### Layer 3 — `splunkconfextension` (conf disk persistence)

Owns conf read/write backed by `$SPLUNK_ETC` on disk.

- **Depends on**: `splunkhttpextension` + `splunkframeworkextension`
- **Change**: drop `setInMemoryConfChanges()`; use real `BundlesSetup` with disk backing
- **Activates**: `NoahConfiguration`, `JSONConfPathMapper`, `ConfObjectManagerDB`,
  `ConfPathMapper` — all check `inMemoryConfChanges()` before writing
- **Needed by**: components that persist config changes across restarts

### What each layer unsuppresses

| Subsystem | Suppressed by today | Unsuppressed by |
|---|---|---|
| OpenSSL global init | cabi skips `SslContext::globalInit()` | `splunksslextension` |
| HTTP server / REST | not started | `splunkhttpextension` |
| Auth (tokens, Duo2FA, LDAP) | `setTestMode()` | `splunkhttpextension` (calls `unsetTestMode()`) |
| Conf disk writes | `setInMemoryConfChanges()` | `splunkconfextension` |
| DMC runner | `setTestMode()` | separate extension if needed |
| SHPooling | `setTestMode()` | separate extension if needed |
| IPC broker | `setTestMode()` | separate extension if needed |
| Splunk's own OTel provider | `setTestMode()` | separate extension if needed |
| CLI (`splunk btool`, `splunk search`) | always a separate process | **not an extension** — CLI always `exec()`s; no persistent lifecycle |

---

## Approach B — Embed splunkd as a sidecar process

Instead of porting each subsystem individually, run a **full splunkd process** alongside
the OTel Collector and pipe data between them.

```
┌─────────────────────────────────────────┐
│  OTel Collector                         │
│                                         │
│  splunktailreceiver  (pure Go, no CGo)  │
│    reads files, emits OTLP logs         │
│         │                               │
│         ▼                               │
│  splunkd forwarder receiver             │
│    receives OTLP or HEC or syslog       │
│         │                               │
│  [all Splunk capabilities available:    │
│   routing, props, transforms, SSL,      │
│   conf management, deployment client]   │
│         │                               │
└─────────┼───────────────────────────────┘
          │  S2S TCP (existing protocol)
          ▼
     Splunk indexer
```

Or inverted — splunkd as the supervisor, with the OTel Collector embedded or
communicating via HEC/syslog/S2S:

```
splunkd (full process)
    ├─ TailManager (reads files, applies props/transforms)
    ├─ TCP output (S2S to indexer)
    └─ HEC or syslog output → OTel Collector
                                  └─ any OTel pipeline
```

### Comparison

| | Approach A (OTel Extensions) | Approach B (splunkd sidecar) |
|---|---|---|
| **Splunk capabilities** | Only what is ported; each subsystem requires a new extension | Full splunkd available immediately — routing, props, transforms, SSL, deployment client, conf management, clustering |
| **OTel integration** | Native — receiver/exporter/extension model; pipelines compose freely | Protocol boundary (HEC, syslog, S2S, OTLP); two process lifecycles to manage |
| **Operational complexity** | One binary, one config file, one lifecycle | Two processes, two config systems (`collector.yaml` + `inputs.conf`/`outputs.conf`) |
| **Upgrade path** | splunkd and OTel versions decouple — can upgrade independently | splunkd and OTel upgrade together; version skew at the protocol boundary |
| **Resource overhead** | Single process; shared memory | Two processes; two copies of Splunk framework loaded |
| **Performance impact** | Without `splunkframeworkextension`: each CGo lib runs its own `EventLoop` (tailin's full `SplunkMainThread` eloop + tcpout's per-worker eloop) — redundant timer/poll cycles, extra memory, extra context switches. With the shared extension: one `EventLoop` drives all components, eliminating the duplication. No serialization overhead. | Inter-process data copy on every event (S2S/HEC/OTLP serialization + TCP loopback) — measurable latency (sub-ms on loopback but nonzero) and CPU cost for encode/decode. Throughput capped by the IPC socket. |
| **Splunk conf management** | Not available until `splunkconfextension` built | Available immediately via `splunkd` REST API and `btool` |
| **Deployment client** | Not ported | Available in splunkd; pushes confs from Deployment Server |
| **Debugging** | OTel traces, metrics, logs | Splunk `splunkd.log`, `metrics.log`, REST `/_raw` introspection |
| **When to choose** | Long-term goal: full OTel-native pipeline with Splunk engine | Short-term: maximum Splunk capability with minimal porting work |

### Practical hybrid

The most practical near-term architecture is a **hybrid**:

```
splunkd (UF, lightweight)          OTel Collector
    TailManager ──────────────────► splunktailreceiver (or otlp receiver)
    TCP output (S2S)                splunktcpoutexporter
    Deployment client               (managed by splunkd conf push)
    props/transforms                (splunkd handles before forwarding)
```

- splunkd handles everything it's already good at: file tailing with transforms,
  deployment server sync, conf management, SSL certificate management
- OTel handles routing, enrichment, fan-out, and integration with non-Splunk systems
- The boundary is a well-defined protocol (S2S, HEC, or OTLP) — no CGo, no shared
  singletons, no porting required

The individual extension approach (Approach A) becomes worthwhile when you want to
**eliminate the splunkd dependency entirely** — e.g. shipping a single OTel binary
that replaces the UF without requiring a Splunk installation.

---

## Summary: which approach when

| Goal | Option | Recommended approach |
|---|---|---|
| Get tail reading + S2S forwarding working now | — | **Two-binary OTLP bridge** (already working) |
| Single binary, minimal code change, quickest unblock | **Option A** | `-fvisibility=hidden` on both `.so` builds — no C++ changes |
| Single binary, clean OTel ownership, best performance | **Option B** | `splunkframeworkextension` (Layer 0 only) — shared `SplunkMainThread` |
| Single binary, full Splunk capabilities, no porting | **Option C** | splunkd sidecar — connect via S2S / HEC / OTLP |
| Incrementally add more Splunk subsystems to OTel | **Option B** | Add extension layers 1–3 (SSL, HTTP, conf) as needed |
| Replace UF entirely, no Splunk installation required | **Option B** | All layers fully ported |
| Long-term: eliminate C++ / CGo dependency entirely | **Option D** | Rewrite in Go — same shared-state design questions as A/B apply; full feature parity is a large multi-year effort |
