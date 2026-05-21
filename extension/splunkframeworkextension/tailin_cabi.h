/**
 * tailin_cabi.h
 *
 * Plain C interface to the Splunk tail-input library.
 * This header is the ONLY thing callers need — no Splunk headers required.
 *
 * This wraps the real Splunk TailReader / TailWatcher / WatchedTailFile
 * pipeline, including fish-bucket position persistence, CRC-based file
 * identity, log-rotation detection, line-breaking, charset detection,
 * archive reading, and props.conf transforms.
 *
 * Model: callback-based (push to caller).
 * The library owns a parsing-pipeline thread.  For each fully line-broken
 * event read from a watched file, tailin_event_cb is called from that
 * thread.  The callback must return quickly; heavy work should be
 * dispatched to a separate goroutine/thread.
 *
 * Usage (C / C++):
 *   #include "tailin_cabi.h"
 *
 *   void on_event(const char* data, size_t len,
 *                 const char* source,
 *                 const char* sourcetype,
 *                 const char* host,
 *                 time_t      event_time,
 *                 void*       userdata) {
 *       // handle event
 *   }
 *
 *   TailinConfig cfg = tailin_default_config();
 *   cfg.default_sourcetype = "myapp";
 *   TailinHandle* h = tailin_create(on_event, NULL, &cfg);
 *   tailin_add_monitor(h, "/var/log/myapp/*.log", "myapp", NULL, NULL);
 *   tailin_start(h);
 *   // ... later ...
 *   tailin_stop(h);
 *   tailin_destroy(h);
 *
 * Usage (Go via CGo):
 *   // #cgo LDFLAGS: -L. -ltailinput_cabi -lstdc++ -ldl
 *   // #include "tailin_cabi.h"
 *   import "C"
 */

#ifndef TAILIN_CABI_H
#define TAILIN_CABI_H

#include <stddef.h>   /* size_t  */
#include <stdint.h>   /* uint64_t */
#include <time.h>     /* time_t  */

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque session handle returned by tailin_create(). */
typedef struct TailinHandle TailinHandle;

/**
 * tailin_event_cb — called once per fully line-broken event.
 *
 * @param data        Event bytes (NUL-terminated for convenience).
 * @param len         Length of @p data in bytes (excluding the NUL).
 * @param source      Absolute path of the file (never NULL).
 * @param sourcetype  Sourcetype field value (never NULL).
 * @param host        Host field value (never NULL).
 * @param event_time  Unix timestamp assigned by the Splunk pipeline.
 *                    0 if not set.
 * @param userdata    The pointer passed to tailin_create().
 *
 * The callback is invoked from the parsing-pipeline thread.  Do not
 * call any tailin_* functions from within the callback.
 */
typedef void (*tailin_event_cb)(
    const char* data,
    size_t      len,
    const char* source,
    const char* sourcetype,
    const char* host,
    time_t      event_time,
    void*       userdata
);

/**
 * TailinConfig — optional top-level configuration.
 * Initialise with tailin_default_config() before setting fields.
 */
typedef struct TailinConfig {
    /**
     * Default sourcetype for monitors that don't specify one.
     * NULL or "" → "tailin".
     */
    const char* default_sourcetype;

    /**
     * Host field value.  NULL or "" → local hostname (gethostname).
     */
    const char* host;

    /**
     * Default Splunk index for all monitors.
     * NULL or "" → use the index from the monitor stanza, or "main".
     */
    const char* default_index;

    /**
     * Path to a writable directory for fish-bucket state files.
     * NULL or "" → $SPLUNK_DB/fishbucket (from LoaderInfo / SPLUNK_HOME).
     */
    const char* fishbucket_dir;
} TailinConfig;

/**
 * tailin_default_config() — return a TailinConfig with sensible defaults.
 */
TailinConfig tailin_default_config(void);

/**
 * tailin_create() — create a tail-input handle (does NOT start tailing yet).
 *
 * Bootstraps the Splunk framework (Logger, LoaderInfo, BundlesSetup,
 * PropertyPages, PipelineComponent) and registers a single PipelineSet.
 * This must be called at most once per process.
 *
 * @param cb         Event callback, called once per line-broken event.
 * @param userdata   Opaque pointer forwarded to every cb invocation.
 * @param cfg        Optional configuration; pass NULL for defaults.
 *
 * @return  Opaque handle on success, NULL on failure.
 *          Call tailin_last_error() for a human-readable reason.
 */
TailinHandle* tailin_create(tailin_event_cb    cb,
                             void*              userdata,
                             const TailinConfig* cfg);

/**
 * tailin_add_monitor() — register a programmatic glob pattern to watch.
 *
 * Must be called after tailin_create() and before tailin_start().
 * Optional: callers may skip this entirely when they want TailManager to use
 * the merged inputs.conf cache loaded from SPLUNK_HOME.
 * Writes an in-memory inputs.conf stanza:
 *   [monitor://<glob>]
 *   sourcetype = <sourcetype>
 *   index      = <index>
 *
 * @param handle      Handle returned by tailin_create().
 * @param glob        Shell-style glob or exact path, e.g. "/var/log/*.log".
 * @param sourcetype  Sourcetype override; NULL/""  → default_sourcetype.
 * @param index       Index override; NULL/"" → default_index.
 * @param host        Host override for this monitor; NULL/"" → global host.
 *
 * @return  0 on success, -1 on error (see tailin_last_error()).
 */
int tailin_add_monitor(TailinHandle* handle,
                       const char*   glob,
                       const char*   sourcetype,
                       const char*   index,
                       const char*   host);

/**
 * tailin_start() — start the TailManager, TailReader, and parsing pipeline.
 *
 * Must be called exactly once after tailin_create() and any
 * tailin_add_monitor() calls. If no monitors were added programmatically,
 * the native TailManager reads the already-loaded merged inputs.conf cache.
 *
 * @return  0 on success, -1 on error (see tailin_last_error()).
 */
int tailin_start(TailinHandle* handle);

/**
 * tailin_stop() — stop all threads and wait for them to exit.
 *
 * After this call no more callbacks will be issued.
 * tailin_destroy() must still be called to free resources.
 */
void tailin_stop(TailinHandle* handle);

/**
 * tailin_destroy() — free all resources associated with the handle.
 *
 * tailin_stop() must have been called first (or tailin_start() was
 * never called).
 */
void tailin_destroy(TailinHandle* handle);

/**
 * tailin_last_error() — human-readable description of the last error.
 *
 * The returned pointer is valid until the next call to any tailin_*
 * function from the same thread.
 */
const char* tailin_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* TAILIN_CABI_H */
