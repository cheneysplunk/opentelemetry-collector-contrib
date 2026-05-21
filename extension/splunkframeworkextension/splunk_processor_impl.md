# Splunk Component Porting Playbook

This document is a step-by-step implementation guide for porting Splunk C++
subsystems into the OTel Collector as receivers, exporters, or processors.
It is written so that an AI agent can follow it sequentially to reproduce the
work from scratch.

**Related reference docs** (read these for exact source lists, object counts, and library deps):

| Doc | Location |
|---|---|
| `tail_lib` deps & CABI API | [`main/src/input/tail_lib/README.md`](../../../../../main/src/input/tail_lib/README.md) |
| `tcpout_lib` deps & CABI API | [`main/src/output/tcpout_lib/README.md`](../../../../../main/src/output/tcpout_lib/README.md) |
| `framework_cabi` build & symbol inventory | [`main/src/framework_cabi/README.md`](../../../../../main/src/framework_cabi/README.md) |

---

## Table of Contents

1. [Repository layout](#1-repository-layout)
2. [Phase 1 — Componentize a Splunk subsystem (tcpout walkthrough)](#2-phase-1--componentize-a-splunk-subsystem)
3. [Phase 2 — Merge components into libsplunk_cabi.so](#3-phase-2--merge-components-into-libsplunk_cabiso)
4. [Phase 3 — Build splunkframeworkextension](#4-phase-3--build-splunkframeworkextension)
5. [Phase 4 — Build a receiver based on the extension](#5-phase-4--build-a-receiver)
6. [Phase 5 — Build an exporter based on the extension](#6-phase-5--build-an-exporter)
7. [Phase 6 — Processors — what NOT to do](#7-phase-6--processors)
8. [Lifecycle contract summary](#8-lifecycle-contract-summary)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Repository layout

```
main/                              # Splunk source tree (splcore)
  src/
    framework/                     # SplunkMainThread, EventLoop, PropertyPages
    input/
      tail_lib/                    # TailReader, WatchedTailFile, TailWatcher
        tailin_cabi.h              # plain-C CABI header for tail input
        tailin_cabi.cpp            # C++ wrapper calling internal classes
        Makefile                   # builds libtailinput_core.a + standalone .so
    output/
      tcpout_lib/                  # TcpOutputProcessorImpl, QueueManager
        tcpout_cabi.h              # plain-C CABI header for tcp output
        tcpout_cabi.cpp            # C++ wrapper calling internal classes
        Makefile                   # builds libtcpout.a + libsupport.a
    framework_cabi/                # unified CABI layer (the bridge)
      splunkfw_cabi.h / .cpp       # framework bootstrap: splunkfw_init / shutdown
      tailin_cabi.h  (symlink)     # re-exported from tail_lib
      tcpout_cabi.h  (symlink)     # re-exported from tcpout_lib
      splunk_pipeline_cabi.h / .cpp  # conf-text router (calls tailin + tcpout)
      Makefile                     # builds libsplunk_cabi.so (unified)
      libsplunk_cabi.so            # OUTPUT: single .so loaded by OTel collector

splunk_home/                       # Splunk runtime: etc/, lib/, ...
splunk_build/                      # CMake build artefacts (object files)

otel/opentelemetry-collector-contrib/
  extension/splunkframeworkextension/   # OTel extension (CGo, owns libsplunk_cabi.so)
    extension.go                        # Linux CGo implementation
    splunkfw_cabi.h                     # vendored copy
    splunk_pipeline_cabi.h              # vendored copy
    splunkapi/api.go                    # pure-Go interfaces
  receiver/splunktailreceiver/          # OTel receiver (CGo, calls tailin_* directly)
    receiver.go
    tailin_cabi.h                       # vendored copy
    splunkfw_cabi.h                     # vendored copy
  exporter/splunktcpoutexporter/        # OTel exporter (pure Go via extension)
    exporter.go
    demo/
      builder-config-unified.yaml       # ocb build manifest
      config-unified.yaml               # runtime config
      bin-unified/splunk-col            # OUTPUT: unified collector binary
```

---

## 2. Phase 1 — Componentize a Splunk subsystem

### Goal

Isolate one Splunk C++ subsystem (e.g. `tcpout`, `tailin`) behind a **plain-C
ABI boundary** so it can be called from Go via CGo without exposing any C++
types to the linker or to Go.

### Why a plain-C ABI is mandatory

CGo only understands C types.  C++ name mangling, vtables, exceptions, and
`std::*` types cannot cross the CGo boundary.  Every function in `*_cabi.h`
must be declared `extern "C"` and use only plain-C types (`char*`, `int`,
`size_t`, `uint8_t*`, opaque `struct` pointers).

### Step-by-step: new component `foo`

#### 2a. Create the component directory

Create a new directory **next to** the Splunk source, not inside it:

```
main/src/output/tcpout_lib/      ← Splunk-owned source (do not modify)
main/src/output/tcpout_lib/tcpout_cabi.h     ← NEW plain-C header
main/src/output/tcpout_lib/tcpout_cabi.cpp   ← NEW C++ wrapper
main/src/output/tcpout_lib/Makefile          ← NEW build file
```

For a hypothetical `foo` subsystem:

```
main/src/foo_lib/
  foo_cabi.h       # plain-C ABI declarations
  foo_cabi.cpp     # C++ wrapper including internal headers
  test_foo.cpp     # standalone smoke test (no OTel)
  Makefile
```

#### 2b. Write `foo_cabi.h` (the stable ABI)

Rules:
- Include only `<stddef.h>`, `<stdint.h>`, `<time.h>` — no Splunk headers
- All functions `extern "C"` (wrap the whole file in `#ifdef __cplusplus`)
- Opaque handle: `typedef struct FooHandle FooHandle;`
- No C++ types: no `std::string`, no templates, no references, no exceptions

```c
#ifndef FOO_CABI_H
#define FOO_CABI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Guard: must be called only after splunkfw_init() returns 0. */
typedef struct FooHandle FooHandle;

/**
 * foo_create() — allocate and initialise a Foo session.
 * Returns NULL on failure; call foo_last_error() for reason.
 */
FooHandle* foo_create(const char* config_param);

/** foo_do_work() — perform the core operation. */
int foo_do_work(FooHandle* h, const uint8_t* data, size_t len);

/** foo_destroy() — flush and shut down. */
void foo_destroy(FooHandle* h, int drain_secs);

/** foo_last_error() — human-readable last error string. */
const char* foo_last_error(void);

#ifdef __cplusplus
}
#endif
#endif /* FOO_CABI_H */
```

#### 2c. Write `foo_cabi.cpp` (the C++ wrapper)

Key patterns to follow:

```cpp
// foo_cabi.cpp
#include "foo_cabi.h"
#include "splunkfw_cabi.h"   // for splunkfw_is_running()

// Include internal Splunk C++ headers freely here — they never cross the ABI
#include "framework/foo/FooProcessor.h"
#include "framework/foo/FooConfig.h"

#include <string>
#include <mutex>

// Thread-local error string — safe for concurrent callers
static thread_local std::string s_last_error;

static void setError(const std::string& msg) { s_last_error = msg; }
const char* foo_last_error(void) { return s_last_error.c_str(); }

struct FooHandle {
    FooProcessor* impl = nullptr;
    std::mutex    mu;
};

FooHandle* foo_create(const char* config_param) {
    // Always check framework is running before calling any Splunk class
    if (!splunkfw_is_running()) {
        setError("foo_create: splunkfw_init() has not been called");
        return nullptr;
    }
    auto* h = new FooHandle();
    h->impl = FooProcessor::create(config_param);
    if (!h->impl) {
        setError("foo_create: FooProcessor::create failed");
        delete h;
        return nullptr;
    }
    return h;
}

int foo_do_work(FooHandle* h, const uint8_t* data, size_t len) {
    if (!h || !h->impl) { setError("foo_do_work: null handle"); return -1; }
    std::lock_guard<std::mutex> lk(h->mu);
    return h->impl->process(data, len) ? 0 : -1;
}

void foo_destroy(FooHandle* h, int drain_secs) {
    if (!h) return;
    h->impl->shutdown(drain_secs);
    delete h->impl;
    delete h;
}
```

#### 2d. Write the Makefile

Mirror exactly what `tcpout_lib/Makefile` does:

```makefile
# Inherit the same DEFINES, INCLUDES, CXXFLAGS as the main build
# (copy them from tcpout_lib/Makefile — they must be identical)

REPO_ROOT   := $(abspath $(CURDIR)/../../..)
BUILD_ROOT  := $(abspath $(CURDIR)/../../../splunk_build)
SPLUNK_HOME := $(abspath $(CURDIR)/../../../splunk_home)
TCPOUT_DIR  := $(REPO_ROOT)/src/output/tcpout_lib
FW_CABI_DIR := $(REPO_ROOT)/src/framework_cabi

BUILD_DIR   := $(CURDIR)/build
TARGET_SO   := $(CURDIR)/libfoo_cabi.so
TEST_BIN    := $(CURDIR)/test_foo

# ... DEFINES, INCLUDES, CXXFLAGS same as tcpout_lib/Makefile ...

SRCS := $(CURDIR)/foo_cabi.cpp
OBJS := $(BUILD_DIR)/foo_cabi.o

# Prerequisites: libsupport.a must already be built by tcpout_lib
LIB_SUPPORT := $(TCPOUT_DIR)/libsupport.a

$(BUILD_DIR)/foo_cabi.o: $(CURDIR)/foo_cabi.cpp | $(BUILD_DIR)
	@echo "[CC] $<"
	$(Q)$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET_SO): $(OBJS) $(LIB_SUPPORT)
	@echo "[LD] $@"
	$(Q)$(CXX) -shared -fPIC -o $@ $(OBJS) \
	    -Wl,--start-group $(LIB_SUPPORT) -Wl,--end-group \
	    $(LDFLAGS)

# Smoke-test binary — links foo_cabi.o directly (not the .so)
TEST_SRCS := $(CURDIR)/test_foo.cpp
TEST_OBJS := $(BUILD_DIR)/test_foo.o

$(BUILD_DIR)/test_foo.o: $(TEST_SRCS) | $(BUILD_DIR)
	$(Q)$(CXX) $(CXXFLAGS) -I$(FW_CABI_DIR) -c $< -o $@

test_bin: $(TEST_OBJS) $(OBJS) $(LIB_SUPPORT)
	$(Q)$(CXX) -o $(TEST_BIN) $(TEST_OBJS) $(OBJS) \
	    -Wl,--start-group $(LIB_SUPPORT) -Wl,--end-group \
	    $(LDFLAGS) -lstdc++ -ldl -lpthread
```

#### 2e. Write `test_foo.cpp` — the standalone smoke test

**Always write and pass this test before touching any OTel code.**

```cpp
// test_foo.cpp — standalone smoke test, no OTel
#include "foo_cabi.h"
#include "splunkfw_cabi.h"   // from framework_cabi/

#include <cassert>
#include <cstdio>
#include <cstring>

int main() {
    // 1. Init framework
    const char* home = getenv("SPLUNK_HOME");
    assert(home && "set SPLUNK_HOME");
    int rc = splunkfw_init(home, nullptr);
    assert(rc == 0 && "splunkfw_init failed");

    // 2. Create handle
    FooHandle* h = foo_create("my-config");
    if (!h) {
        fprintf(stderr, "foo_create: %s\n", foo_last_error());
        return 1;
    }

    // 3. Exercise the API
    const char* payload = "hello world\n";
    rc = foo_do_work(h, (const uint8_t*)payload, strlen(payload));
    assert(rc == 0);

    // 4. Destroy
    foo_destroy(h, 3);

    // 5. Shut down framework
    splunkfw_shutdown();

    printf("PASS\n");
    return 0;
}
```

Build and run:

```sh
cd main/src/foo_lib
make test_bin
SPLUNK_HOME=/home/chli/splunk_home \
  LD_LIBRARY_PATH=/home/chli/splunk_home/lib \
  ./test_foo
# Expected: PASS (no crash, no ASAN error)
```

**Do not proceed to Phase 2 if this test fails or crashes.**

Common failure modes:
- `splunkfw_is_running()` returns false → forgot to call `splunkfw_init()`
- Segfault in destructor → framework EventLoop already stopped; call
  `splunkfw_shutdown()` only after all handles are destroyed
- Missing symbols → `libsupport.a` not built yet; run `make lib` in `tcpout_lib/` first

---

## 3. Phase 2 — Merge components into `libsplunk_cabi.so`

### Goal

Combine all CABI `.cpp` files into **one shared library** so the OTel collector
loads a single `.so`.  Multiple `.so` files sharing Splunk singleton state
(e.g. `SplunkMainThread::_instance`) will conflict.

### Directory: `main/src/framework_cabi/`

```
framework_cabi/
  splunkfw_cabi.h / .cpp      # framework bootstrap — owns splunkfw_init/shutdown
  splunk_pipeline_cabi.h / .cpp  # conf-text router — delegates to tailin + tcpout
  Makefile                    # builds libsplunk_cabi.so
  libsplunk_cabi.so           # OUTPUT
```

Component CABI sources live in their own directories but are **compiled into
`libsplunk_cabi.so`** (not their own `.so`):

```makefile
# framework_cabi/Makefile (excerpt)
SRCS := \
    $(CURDIR)/splunkfw_cabi.cpp \
    $(CURDIR)/splunk_pipeline_cabi.cpp \
    $(TAIL_DIR)/tailin_cabi.cpp \
    $(TCPOUT_DIR)/tcpout_cabi.cpp
    # add new components here: $(FOO_DIR)/foo_cabi.cpp

LIB_TAILIN_CORE := $(TAIL_DIR)/libtailinput_core.a
LIB_TCPOUT      := $(TCPOUT_DIR)/libtcpout.a
LIB_SUPPORT     := $(TCPOUT_DIR)/libsupport.a

$(TARGET_SO): $(OBJS) $(LIB_TAILIN_CORE) $(LIB_TCPOUT) $(LIB_SUPPORT)
	$(CXX) -shared -fPIC -o $@ $(OBJS) \
	    -Wl,--start-group \
	    $(LIB_TAILIN_CORE) $(LIB_TCPOUT) $(LIB_SUPPORT) \
	    -Wl,--end-group \
	    $(LDFLAGS)
```

### Adding component `foo` to the unified `.so`

1. Add `$(FOO_DIR)/foo_cabi.cpp` to `SRCS` in `framework_cabi/Makefile`
2. Add `$(FOO_DIR)/libfoo_core.a` (if any) to the link group
3. Add a stanza branch in `splunk_pipeline_cabi.cpp`:

```cpp
// In splunk_pipeline_create_bytes(), after the tailin block:
for (auto& s : ...) {
    if (!starts_with(s.name, "foo://")) continue;
    p->foo = foo_create(s.kv["config"].c_str());
    if (!p->foo) { /* error */ return nullptr; }
    break;
}
```

4. Add `FooHandle* foo = nullptr;` to `SplunkPipeline` struct
5. Call `foo_destroy(p->foo, drain_secs)` in `splunk_pipeline_destroy()`
6. Rebuild: `cd main/src/framework_cabi && make -j$(nproc)`

### Verify symbol presence

```sh
readelf -Ws main/src/framework_cabi/libsplunk_cabi.so \
  | grep "foo_create\|foo_do_work\|foo_destroy" | grep FUNC
```

All three must appear as `GLOBAL DEFAULT` symbols.

> For the full object inventory, static archive sizes, and library dep list,
> see [`main/src/framework_cabi/README.md`](../../../../../main/src/framework_cabi/README.md).

### Vendor headers into the extension

Copy the new CABI header into the OTel extension directory:

```sh
cp main/src/foo_lib/foo_cabi.h \
   otel/opentelemetry-collector-contrib/extension/splunkframeworkextension/
```

If the header is used by a receiver or processor, also copy it there:

```sh
cp main/src/foo_lib/foo_cabi.h \
   otel/opentelemetry-collector-contrib/receiver/splunkfooreceiver/
```

---

## 4. Phase 3 — Build `splunkframeworkextension`

### Role

The extension:
- Calls `splunkfw_init()` in `Start()` — boots the Splunk framework once per process
- Calls `splunkfw_shutdown()` in `Shutdown()` — drains and stops the framework
- Implements `splunkapi.SplunkFramework.NewPipeline()` — conf-text-driven factory
- Is the **only** component in the binary that loads `libsplunk_cabi.so` CGo symbols

### Key implementation rules

**`Start()` (extension)**
```go
func (e *splunkFrameworkExtension) Start(_ context.Context, _ component.Host) error {
    // Set SPLUNK_HOME env before C init
    cHome := C.CString(e.cfg.SplunkHome)
    defer C.free(unsafe.Pointer(cHome))
    rc := C.splunkfw_init(cHome, nil)   // std::call_once — idempotent
    if rc != 0 {
        return fmt.Errorf("splunkfw_init failed: %s", C.GoString(C.splunkfw_last_error()))
    }
    return nil
}
```

**`Shutdown()` (extension)**
```go
func (e *splunkFrameworkExtension) Shutdown(_ context.Context) error {
    C.splunkfw_shutdown()   // checks s_smtRunning — no-op if already stopped
    return nil
}
```

**`NewPipeline()` — outputs-only example (exporter use-case)**
```go
func (e *splunkFrameworkExtension) NewPipeline(cfg splunkapi.PipelineConfig) (splunkapi.Pipeline, error) {
    var cOutputs *C.char
    if cfg.OutputsConf != "" {
        cOutputs = C.CString(cfg.OutputsConf)
        defer C.free(unsafe.Pointer(cOutputs))
    }

    // For output-only pipelines: cb and userdata are nil
    handle := C.splunk_pipeline_create_bytes(nil, cOutputs, nil, nil, nil)
    if handle == nil {
        return nil, fmt.Errorf("splunk_pipeline_create failed: %s",
            C.GoString(C.splunk_pipeline_last_error()))
    }
    return &cPipeline{handle: handle}, nil
}
```

**`NewPipeline()` — inputs-only example (receiver use-case, if routing via extension)**
```go
// Integer-as-pointer: vet-clean pattern for passing int IDs as void*
id := pipelineNextID.Add(1)
ch := make(chan splunkapi.Event, 1024)
pipelineRegistry.Store(id, ch)
cb := C.splunk_bytes_cb(C.goPipelineBytesCallback)
idWord := uintptr(id)
udPtr := *(*unsafe.Pointer)(unsafe.Pointer(&idWord))
handle := C.splunk_pipeline_create_bytes(cInputs, nil, nil, cb, udPtr)
```

### Build the extension

```sh
FW_DIR=/home/chli/main/src/framework_cabi
SPLUNK_HOME=/home/chli/splunk_home

cd otel/opentelemetry-collector-contrib/extension/splunkframeworkextension

CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi \
             -Wl,-rpath,${FW_DIR} \
             -Wl,-rpath,${SPLUNK_HOME}/lib \
             -lstdc++ -ldl -lpthread" \
go build ./... && go vet ./...
```

### CGo preamble rules

The `extension.go` CGo preamble **must** include `<stdint.h>` (for `uint8_t`),
and any header that defines a callback type:

```go
/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -lsplunk_cabi -lstdc++ -ldl -lpthread
#include "splunkfw_cabi.h"
#include "splunk_pipeline_cabi.h"
#include <stdlib.h>
#include <stdint.h>

// Forward declarations for Go-exported callbacks
extern void goPipelineBytesCallback(uint8_t* data, size_t len,
                                    char* source, char* sourcetype,
                                    char* host, time_t event_time,
                                    void* userdata);
*/
import "C"
```

---

## 5. Phase 4 — Build a receiver

### Architecture choice

There are two valid approaches:

| Approach | Description | Use when |
|---|---|---|
| **A — Via extension** | Receiver calls `fw.NewPipeline(InputsConf: ...)` | Extension owns lifecycle |
| **B — Direct CGo** | Receiver calls `tailin_create_bytes()` directly | Need full control (current `splunktailreceiver`) |

`splunktailreceiver` uses **Approach B**: it calls `tailin_*` CGo directly and
calls `splunkfw_init()` itself (idempotent — `std::call_once` guards it).

### Key implementation rules for receivers

**`Start()`**
```go
func (r *splunkTailReceiver) Start(ctx context.Context, host component.Host) error {
    // 1. Call splunkfw_init() — idempotent if extension already called it
    if rc := C.splunkfw_init(cHome, nil); rc != 0 {
        return fmt.Errorf("splunkfw_init: %s", C.GoString(C.splunkfw_last_error()))
    }
    // 2. Create tailin handle
    r.handle = C.tailin_create_bytes(C.tailinCBPtr(), udPtr, &cfg)
    // 3. Add monitors
    for _, m := range r.cfg.Monitors {
        C.tailin_add_monitor(r.handle, glob, stype, index, host)
    }
    // 4. Start tailing
    C.tailin_start(r.handle)
    // 5. Start Go consume loop
    go r.consumeLoop(ctx)
    return nil
}
```

**`Shutdown()` — critical ordering**
```go
func (r *splunkTailReceiver) Shutdown(ctx context.Context) error {
    // 1. Stop C-level tailing (no more callbacks after this)
    C.tailin_stop(r.handle)
    // 2. Close the event channel (signals consumeLoop to drain and exit)
    close(r.chunkCh)
    // 3. Free C-level resources
    C.tailin_destroy(r.handle)
    r.handle = nil
    // 4. DO NOT call splunkfw_shutdown() here.
    //    The OTel collector shuts down receivers BEFORE extensions.
    //    The extension's Shutdown() calls splunkfw_shutdown() after all
    //    receivers have stopped — that is the correct ordering.
    //    Calling it here would tear down the EventLoop while the
    //    tcpout exporter is still draining its send queue.
    r.cancel()
    r.wg.Wait()
    return nil
}
```

**CGo callback trampoline**

Go functions exported via `//export` cannot be cast directly to C function
pointer types.  Use a static C bridge:

```go
/*
// Bridge: Go cannot cast an //export function to a C function pointer type directly.
// This static helper is visible to the C compiler and can be safely cast.
static tailin_bytes_cb tailinCBPtr(void) {
    return (tailin_bytes_cb)tailinGoCallback;
}
*/
import "C"

//export tailinGoCallback
func tailinGoCallback(data unsafe.Pointer, length C.size_t,
    source, sourcetype, host *C.char, eventTime C.time_t, userdata unsafe.Pointer) {
    // recover the receiver ID from the integer-as-pointer
    idWord := uintptr(userdata)     // safe: we stored uintptr, not a Go pointer
    id := int32(idWord)
    // push chunk to the Go channel
    r := receiverRegistry.Load(id).(*splunkTailReceiver)
    chunk := C.GoBytes(data, C.int(length))
    r.chunkCh <- chunkEvent{data: chunk, source: C.GoString(source), ...}
}
```

**Integer-as-pointer (vet-clean)**

Passing an integer ID as `void* userdata` must be done without triggering
`go vet`'s unsafe.Pointer checker:

```go
// WRONG (vet error: possible misuse of unsafe.Pointer):
udPtr = unsafe.Pointer(uintptr(id))

// CORRECT:
idWord := uintptr(id)
udPtr = *(*unsafe.Pointer)(unsafe.Pointer(&idWord))
```

### Body type emitted by receiver

Always set `ValueTypeBytes`:

```go
lr.Body().SetEmptyBytes().FromRaw(ev.data)
```

Never use `SetStr()` for binary/multi-line chunks — it would re-encode to UTF-8
and could corrupt binary data or allocate unnecessarily.

---

## 6. Phase 5 — Build an exporter

### Architecture

The exporter is **pure Go** (no CGo).  It obtains the `splunkapi.SplunkFramework`
interface from the extension at startup:

```go
func (e *splunktcpoutExporter) start(_ context.Context, host component.Host) error {
    ext, ok := host.GetExtensions()[e.cfg.Framework]
    if !ok {
        return fmt.Errorf("extension %q not found", e.cfg.Framework)
    }
    fw := ext.(splunkapi.SplunkFramework)

    p, err := fw.NewPipeline(splunkapi.PipelineConfig{
        OutputsConf: buildOutputsConf(e.cfg),
    })
    if err != nil {
        return fmt.Errorf("splunk_pipeline_create failed: %w", err)
    }
    if err := p.Start(); err != nil {
        p.Destroy()
        return err
    }
    e.pipeline = p
    return nil
}
```

**`buildOutputsConf()`** — produce the raw stanza text the C router expects:

```go
func buildOutputsConf(cfg *Config) string {
    // [tcpout] header stanza (sets defaultGroup)
    s := "[tcpout]\ndefaultGroup = cabi\n"
    // [tcpout:cabi] group stanza (sets server = host:port)
    s += fmt.Sprintf("[tcpout:cabi]\nserver = %s:%d\n", cfg.Host, cfg.Port)
    if cfg.Index != "" {
        s += "index = " + cfg.Index + "\n"
    }
    return s
}
```

The C router (`splunk_pipeline_cabi.cpp`) parses `[tcpout:GROUPNAME]` stanzas
and calls `tcpout_create(host, port, index)`.

### Handling body bytes

```go
func logBodyBytes(lr plog.LogRecord) []byte {
    b := lr.Body()
    switch b.Type() {
    case pcommon.ValueTypeBytes:
        return b.Bytes().AsRaw()   // zero-copy: slice into pdata buffer
    case pcommon.ValueTypeStr:
        return []byte(b.Str())     // one allocation
    default:
        return []byte(b.AsString()) // fallback
    }
}
```

### `Shutdown()` — exporter**

```go
func (e *splunktcpoutExporter) shutdown(_ context.Context) error {
    if e.pipeline != nil {
        e.pipeline.Stop(e.cfg.DrainSeconds)  // flush send queue
        e.pipeline.Destroy()
        e.pipeline = nil
    }
    return nil
}
```

The exporter must **not** call `splunkfw_shutdown()`.  Only the extension calls it.

---

## 7. Phase 6 — Processors

A **processor** sits between receiver and exporter in the OTel pipeline.  It
receives `plog.Logs`, transforms or filters records, and passes them downstream.

### What processors must NOT do

| Action | Reason |
|---|---|
| Call `splunkfw_init()` | Extension already called it; calling again is harmless (idempotent) but misleading |
| Call `splunkfw_shutdown()` | **Fatal** — shuts down the framework while receiver and exporter are still running |
| Call `splunkfw_*` or `tailin_*` or `tcpout_*` CGo directly | Processors should be pure Go; use the extension interface if C is needed |
| Hold `libsplunk_cabi.so` CGo | Only the extension and receiver/exporter that explicitly need it should link CGo |

### Lifecycle for a Splunk-aware processor

If a processor needs to call a Splunk subsystem (e.g. to enrich events with
conf data), obtain the framework via the extension interface — do not link CGo:

```go
func (p *myProcessor) Start(_ context.Context, host component.Host) error {
    ext, ok := host.GetExtensions()[p.cfg.Framework]
    fw := ext.(splunkapi.SplunkFramework)
    // Use fw.NewPipeline() if needed — but most processors won't need this
    return nil
}

func (p *myProcessor) Shutdown(_ context.Context) error {
    // Clean up processor state only.
    // DO NOT call splunkfw_shutdown() or any C function.
    return nil
}
```

### Processor `ConsumeLogs()` example

```go
func (p *myProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
    // Filter, enrich, route — pure Go
    for i := 0; i < ld.ResourceLogs().Len(); i++ {
        rl := ld.ResourceLogs().At(i)
        for j := 0; j < rl.ScopeLogs().Len(); j++ {
            sl := rl.ScopeLogs().At(j)
            for k := 0; k < sl.LogRecords().Len(); k++ {
                lr := sl.LogRecords().At(k)
                // Example: add attribute
                lr.Attributes().PutStr("processed.by", "myprocessor")
            }
        }
    }
    return p.nextConsumer.ConsumeLogs(ctx, ld)
}
```

---

## 8. Lifecycle contract summary

The OTel collector starts and stops components in this order:

```
Start order:    extensions → exporters → processors → receivers
Shutdown order: receivers  → processors → exporters → extensions
```

Consequence for Splunk components:

| Component | `Start()` | `Shutdown()` |
|---|---|---|
| `splunkframeworkextension` | `splunkfw_init()` | `splunkfw_shutdown()` |
| `splunktailreceiver` | `splunkfw_init()` (idempotent) + `tailin_create_bytes()` + `tailin_start()` | `tailin_stop()` + close channel + `tailin_destroy()` + **no** `splunkfw_shutdown()` |
| `splunktcpoutexporter` | `fw.NewPipeline(OutputsConf)` + `p.Start()` | `p.Stop(drainSecs)` + `p.Destroy()` |
| Any processor | optional `fw` lookup | clean up own state only |

The extension's `Shutdown()` runs **last** — after all receivers have stopped
sending and all exporters have drained their queues — so `splunkfw_shutdown()`
safely tears down the EventLoop with no pending work.

---

## 9. Troubleshooting

### `splunk_pipeline_create failed: both inputs_conf and outputs_conf are empty`

The C router received `NULL` (or empty string) for both `inputs_conf` and
`outputs_conf`.  Check:
- `buildOutputsConf()` returns a non-empty string
- The stanza header format: `[tcpout:GROUPNAME]` (colon, not slash)
- The `server` key is present: `server = host:port`

### `splunk_pipeline_create_bytes: tcpout_create failed`

The C router found a `[tcpout:GROUPNAME]` stanza but `tcpout_create()` returned
NULL.  Call `tcpout_last_error()` for the reason.  Common causes:
- `splunkfw_init()` not called before `tcpout_create()`
- Invalid `host:port` format (missing colon, non-numeric port)

### Segfault in `Timeout::~Timeout()` on exit

The process is exiting while the Splunk EventLoop is still running.  Fix:
call `splunkfw_shutdown()` (from the extension's `Shutdown()`) before the
process exits.  The extension must be listed in `service.extensions`.

### `go vet: possible misuse of unsafe.Pointer`

You wrote `unsafe.Pointer(uintptr(id))`.  Replace with:
```go
idWord := uintptr(id)
udPtr := *(*unsafe.Pointer)(unsafe.Pointer(&idWord))
```

### `unknown type name 'uint8_t'` in CGo preamble

Add `#include <stdint.h>` to the CGo preamble and to the vendored
`splunk_pipeline_cabi.h` / `foo_cabi.h` if it is missing.

### Duplicate `splunkfw_*` symbols at runtime

Two `.so` files each containing `splunkfw_init` are loaded in the same process.
All CABI sources must be compiled into the **one** `libsplunk_cabi.so`.
Never load both `libtailinput_cabi.so` and `libsplunk_cabi.so` in the same process.

### New component symbols missing from `libsplunk_cabi.so`

You added `foo_cabi.cpp` to `SRCS` but forgot to rebuild:
```sh
cd main/src/framework_cabi && make -j$(nproc)
readelf -Ws libsplunk_cabi.so | grep foo_create
```

### Extension builds fine but unified binary fails

The ocb builder generates a fresh `go.mod` in `bin-unified/`.  If you edit a
source file after the last build, re-run the full builder command — the builder
does a full `go build`, not incremental.

```sh
cd otel/.../exporter/splunktcpoutexporter/demo
FW_DIR=/home/chli/main/src/framework_cabi
CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi \
             -Wl,-rpath,${FW_DIR} \
             -Wl,-rpath,/home/chli/splunk_home/lib \
             -lstdc++ -ldl -lpthread" \
GOPATH=/home/chli/go \
/home/chli/go/bin/builder --config builder-config-unified.yaml
```
