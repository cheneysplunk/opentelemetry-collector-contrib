# splunktailreceiver — Crash Investigation Notes

## Environment

- Binary: `splunk-col-chunkedlb` (Go + CGo → `libsplunk_cabi.so`)
- Config: `config-chunkedlb.yaml` — monitors `/tmp/e2e_fresh_*.log`
- Fishbucket: **SQLite** (`fishbucket.sqlite.db` + `libsqlite3.so.0.8.6`)
- Pipeline: `splunktailreceiver → chunkedlbprocessor → splunktcpoutexporter`

---

## Symptom

The process crashes with **SIGSEGV** reliably when a second batch of log events
is appended to the monitored file *within the same process lifetime*, after the
first batch has already been delivered and ACKed.

Crash timing (confirmed from multiple core dumps):

1. Start collector.
2. Write 5 events → delivered OK, process alive.
3. Write 5 more events → SIGSEGV within ~8 s.

Clearing the fishbucket WAL/SHM does **not** fix the crash — it happens within a
single run even with a fresh fishbucket.

---

## Core Dump Analysis

### Core: `core-CallbackRunnerT-299221-1778630407`

```
gdb thread list:
  Thread  1 (LWP 299232): rip=0x489c61  → Go runtime.nanotime1  ← signal caught here
  Thread  6 (LWP 299236): PersistentQueue::remove → pthread_cond_timedwait (CORRUPTED condvar)
  Thread 15 (LWP 299238): ReaderThread::main → waiting for jobs
  Thread 16 (LWP 299239): ReaderThread::main → waiting for jobs
  Thread  9 (LWP 299237): TailWatcher::run → EventLoop::run
  Thread 12 (LWP 299233): ShutdownThread::main → EventLoop::run
  Thread  4 (LWP 299231): SplunkMainThread::main → EventLoop::run
```

Thread 6 backtrace (the corrupted thread):

```
#5  PthreadConditionImpl::wait(ConditionMutex&, ConditionWaitTimeout const&)
#6  PersistentQueue::remove(...)
#7  QueueInputProcessor::createPipelineData(CowPipelineData&)
#8  Processor::createMultiPipelineData(PipelineDataVector*)
#9  Pipeline::main()
```

The `pthread_cond_timedwait` call has `private=-385263094` (garbage; expected 0 or -1).
The condvar/mutex addresses (`0x7c4a6c004200`, `0x7c4a6c004228`) have been **corrupted**.

---

## Crash Signal

- Signal: **SIGSEGV** caught by **Go runtime** (`runtime.nanotime1` at `0x489c61`)
- The Go runtime's signal handler sees the corrupted state and re-raises SIGSEGV.
- All Go goroutines show `runtime.settls` (Go TLS setup) or `runtime.nanotime1` —
  these are not the source; Go caught a signal from the corrupted C++ heap.

---

## Lock/Unlock Sequence (from log before crash)

```
Locked key=0xd5ba0334385fe19f to state=0x7c4a60015f50      ← loadFishState (initial)
Retrieving record for key=0xd5ba0334385fe19f
Unlocked key=0xd5ba0334385fe19f locked to state=0x7c4a60015f50  ← readFile eStatusHaveConfirmedEOF
Unable to convert character set 'UTF-8' to UTF8. Using existing content as is.
Locked key=0xd5ba0334385fe19f to state=0x7c4a60015f50      ← storeOpenFd / eStatusWantEOFConfirmation retry
Retrieving record for key=0xd5ba0334385fe19f
Unlocked key=0xd5ba0334385fe19f locked to state=0x7c4a60015f50  ← releaseStoredFd or ~WTF
[CRASH]
```

### Is the double-unlock the crash cause?

**No.** `WTF::unlockInitCrc()` already guards against a second call:

```cpp
void WTF::unlockInitCrc() {
    if (_lockedInitCrc.raw()) {   // ← no-op if already 0
        FileTracker::instance()->unlockKey(_lockedInitCrc, this);
        _lockedInitCrc = 0;
    }
}
```

A second call is a no-op. `FileTracker::unlockKey()` is only reached when
`_lockedInitCrc != 0`, so it cannot throw the `InputException("unlock: key is not
locked")` on the second call.

---

## Root Cause Hypothesis

The crash is **heap corruption** — something writes into memory owned by the Go
runtime (or corrupts the pthread condvar of `PersistentQueue`).

### Most likely candidate: use-after-free via `WatchedTailFile`

After `eStatusFinishedReading`:

1. `TailReader::read()` → `wfs->setFileStatus(STATUS_FINISHED)` → returns
   `ACKNOWLEDGE_CHANGE`.
2. `ReaderThread::main()` calls `fileDispositionFromAnywhere(state, ACKNOWLEDGE_CHANGE)`
   then **`delete _current`** (deletes the `TailingJob`).
3. The `TailingJob` holds a non-owning `WatchedTailFile*`. After deletion the
   `TailWatcher` may put the WFS back on a timer queue.
4. On the next file-change notification (the second batch of events), the WFS is
   re-enqueued. If the FD cache still holds a stale pointer to a `WatchedTailFile`
   that was already destructed, `_openFds.insert(&state)` / `popStoredFdInto` can
   corrupt adjacent heap memory — including the `PersistentQueue` condvar.

### Supporting evidence

- Crash only happens when new events arrive **after** the first batch reaches
  `eStatusFinishedReading` and the WFS transitions through the FD cache.
- `FDCache::trim()` calls `unlockInitCrc()` on cached WFS pointers — if any of
  those pointers are stale, the memory write of `_lockedInitCrc = 0` goes into
  freed heap, corrupting whatever was reallocated there (the condvar).
- Thread 6 (`PersistentQueue`) has a corrupted `private` field in its futex call,
  which is stored inside the condvar struct — exactly what a stale `WatchedTailFile`
  pointer write would corrupt.

---

## Code Paths Involved

### `TailReader.cpp`

| Line | Action |
|------|--------|
| 1912 | `readFile(*wfs)` |
| 1918 | `case eStatusFinishedReading:` → `setFileStatus(STATUS_FINISHED)` |
| 1947 | `retval = ACKNOWLEDGE_CHANGE` |
| 541  | `fileDispositionFromAnywhere(_current->getState(), disp)` |
| 544  | **`delete _current`** ← TailingJob freed here |
| 1786 | `releaseStoredFd()` calls `state.unlockInitCrc()` |

### `WatchedTailFile.cpp`

| Line | Action |
|------|--------|
| 138  | `unlockInitCrc()` — guarded by `if (_lockedInitCrc.raw())`, safe to call twice |
| 145  | `lockInitCrc()` — stores `_lockedInitCrc = initCRC()` |
| 209  | `~WTF()` calls `unlockInitCrc()` then `_statusRoot->removeItem(...)` |
| 1361 | `FDCache::trim()` calls `unlockInitCrc()` on cached pointers |

### `FileTracker.cpp`

| Line | Action |
|------|--------|
| 88   | `unlockKey()` — throws `InputException` if key not found |
| 53   | `lockKey()` — throws `FileAccessException` if locked by different state |

---

## What Does NOT Cause the Crash

- **Double-unlock** — `unlockInitCrc()` is idempotent; second call is a no-op.
- **Fishbucket WAL** — crash occurs even with a fresh fishbucket in the same run.
- **seekCRC mismatch** — only relevant on process restart with stale fishbucket.

---

## Fix Options

### Option A: Guard `FDCache` against stale pointers (safest)

In `FDCache::trim()` and `FDCache::clear()`, check that the `WatchedTailFile`
pointer is still valid before calling `unlockInitCrc()`. This requires a registry
or generation counter on WFS objects.

### Option B: Remove WFS from FD cache before `delete _current`

In `ReaderThread::main()`, ensure `releaseStoredFd()` is called before
`delete _current`. Currently `delete _current` happens after
`fileDispositionFromAnywhere`, but the WFS may still be in `_cache._openFds`.

**In `TailReader.cpp` around line 541-544:**

```cpp
// BEFORE (current):
_current->getWatcher()->fileDispositionFromAnywhere(
        _current->getState(), disp);
// ...
delete _current;

// AFTER (proposed):
// Make sure WFS is evicted from the FD cache before deleting the job.
if (disp == TailWatcher::ACKNOWLEDGE_CHANGE) {
    releaseStoredFd(*(_current->getState()));
}
_current->getWatcher()->fileDispositionFromAnywhere(
        _current->getState(), disp);
delete _current;
```

### Option C: Null out WFS pointer in FD cache on `eStatusFinishedReading`

In `TailReader::read()`, after `setFileStatus(STATUS_FINISHED)` and before
returning `ACKNOWLEDGE_CHANGE`, explicitly call `_cache.popStoredFdInto()` if
the WFS is cached — closing the fd and removing the (soon-to-be-invalid) pointer
from `_openFds` before `delete _current` frees the WFS.

**In `TailReader.cpp` after line 1920:**

```cpp
case Tailing::eStatusFinishedReading:
    wfs->setFileStatus(Tailing::STATUS_FINISHED);
    // SPL-XXXXX: evict from FD cache before returning ACKNOWLEDGE_CHANGE.
    // The job (and its WFS) will be deleted by ReaderThread::main() after
    // this returns. If the WFS is still in _cache._openFds, FDCache::trim()
    // may later dereference the freed pointer and corrupt the heap.
    if (_cache.isStored(*wfs)) {
        ScopedFileDescriptor fdCloser;
        _cache.popStoredFdInto(*wfs, fdCloser);
        wfs->unlockInitCrc();
    }
    retval = TailWatcher::ACKNOWLEDGE_CHANGE;
    break;
```

**Option C is the minimal targeted fix.**

---

## Rebuild Steps

```bash
cd /home/chli/main/src/framework_cabi && make -j$(nproc)
# Then rebuild the Go binary:
cd /home/chli/otel/opentelemetry-collector-contrib/exporter/splunktcpoutexporter/demo
go build -o bin-chunkedlb/splunk-col-chunkedlb .
```

---

## Test Repro Script

```bash
pkill -f splunk-col-chunkedlb 2>/dev/null; sleep 1
printf 'Starting fresh log file\n' > /tmp/e2e_fresh_test.log

cd /home/chli/otel/opentelemetry-collector-contrib/exporter/splunktcpoutexporter/demo
LD_LIBRARY_PATH=/home/chli/main/src/framework_cabi:/home/chli/splunk_home/lib \
SPLUNK_HOME=/home/chli/splunk_home \
./bin-chunkedlb/splunk-col-chunkedlb --config config-chunkedlb.yaml > /tmp/run.log 2>&1 &
PID=$!

sleep 4
for i in $(seq 1 5); do
  echo "event-$i $(date -Iseconds)" >> /tmp/e2e_fresh_test.log
done
sleep 6
kill -0 $PID && echo "ALIVE after batch 1" || echo "DEAD (crash)"

# Trigger crash: second batch
for i in $(seq 6 10); do
  echo "event-$i $(date -Iseconds)" >> /tmp/e2e_fresh_test.log
done
sleep 8
kill -0 $PID 2>/dev/null && echo "ALIVE after batch 2 — FIXED" || echo "CRASHED on batch 2"
```

---

## Status

| Item | Status |
|------|--------|
| Crash reproduced reliably | ✅ |
| Root cause identified | ✅ heap corruption via stale FDCache pointer |
| Fix implemented | ❌ pending |
| Fishbucket format | SQLite (not btree) |
