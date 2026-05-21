/**
 * tailin_cabi.h
 *
 * Plain C interface to the Splunk tail-input library.
 * This header is the ONLY thing callers need — no Splunk headers required.
 *
 * Wraps Stage 1 of the splcore file-input pipeline: TailReader /
 * TailWatcher / WatchedTailFile, including fish-bucket position persistence,
 * CRC-based file identity, log-rotation detection, charset detection,
 * archive reading, and inputs.conf / props.conf configuration.
 *
 * The CABI delivers raw file chunks — identical to what the UF's tcpout
 * processor receives.  The downstream parsing pipeline (line-breaking,
 * header processing, timestamp extraction) is NOT included; the caller
 * is responsible for those steps.
 *
 * Metadata stamped by the tail reader on every chunk (from inputs.conf):
 *   source      = "source::/absolute/path/to/file"
 *   sourcetype  = value from inputs.conf stanza
 *   host        = value from inputs.conf stanza (or gethostname)
 *   index       = value from inputs.conf stanza
 *   timestamp   = file mtime (fallback; real extraction is downstream)
 *
 * Model: callback-based (push to caller).
 * The library owns a minimal pipeline thread (QueueInputProcessor only).
 * For each raw file chunk read from a watched file, tailin_bytes_cb is
 * called from that thread.  The callback must return quickly; heavy work
 * should be dispatched to a separate goroutine/thread.
 *
 * Usage (C / C++):
 *   #include "tailin_cabi.h"
 *
 *   void on_chunk(const uint8_t* data, size_t len,
 *                 const char* source,
 *                 const char* sourcetype,
 *                 const char* host,
 *                 time_t      event_time,
 *                 void*       userdata) {
 *       // data is a raw file chunk — may contain many lines
 *   }
 *
 *   TailinConfig cfg = tailin_default_config();
 *   cfg.default_sourcetype = "myapp";
 *   TailinHandle* h = tailin_create_bytes(on_chunk, NULL, &cfg);
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

/** Opaque session handle returned by tailin_create_bytes(). */
typedef struct TailinHandle TailinHandle;

/**
 * tailin_bytes_cb — called once per raw file chunk.
 *
 * @param data        Raw file chunk bytes.  NOT NUL-terminated.
 *                    May contain many lines; the caller is responsible
 *                    for line-breaking.
 * @param len         Length of @p data in bytes.
 * @param source      File path as "source::/absolute/path" (never NULL).
 * @param sourcetype  Sourcetype from inputs.conf (never NULL).
 * @param host        Host from inputs.conf or gethostname (never NULL).
 * @param event_time  File mtime (fallback; real timestamp extraction is
 *                    downstream).  0 if not set.
 * @param userdata    The pointer passed to tailin_create_bytes().
 *
 * The callback is invoked from the pipeline thread (not the TailReader
 * thread).  Do not call any tailin_* functions from within the callback.
 */
typedef void (*tailin_bytes_cb)(
    const uint8_t* data,
    size_t         len,
    const char*    source,
    const char*    sourcetype,
    const char*    host,
    time_t         event_time,
    void*          userdata
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
 * tailin_create_bytes() — create a tail-input handle (does NOT start tailing yet).
 *
 * Requires the shared Splunk framework to be running (call splunkfw_init()
 * first).  Registers a minimal pipeline (QueueInputProcessor only — no
 * line-breaking, no header processing) that delivers raw file chunks
 * to @p cb.  This must be called at most once per process.
 *
 * @param cb         Bytes callback, called once per raw file chunk.
 *                   The chunk may contain many lines; the caller is
 *                   responsible for line-breaking.
 * @param userdata   Opaque pointer forwarded to every cb invocation.
 * @param cfg        Optional configuration; pass NULL for defaults.
 *
 * @return  Opaque handle on success, NULL on failure.
 *          Call tailin_last_error() for a human-readable reason.
 */
TailinHandle* tailin_create_bytes(tailin_bytes_cb     cb,
                                   void*               userdata,
                                   const TailinConfig* cfg);

/**
 * tailin_set_props() — set an in-memory props.conf key for a sourcetype.
 *
 * Must be called after tailin_create[_bytes]() and before tailin_start().
 * Allows programmatic configuration of props.conf settings such as
 * INDEXED_EXTRACTIONS, DATETIME_CONFIG, LINE_BREAKER, etc. without
 * requiring a props.conf file on disk.
 *
 * Example — enable JSON indexed extraction for sourcetype "myapp":
 *   tailin_set_props(h, "myapp", "INDEXED_EXTRACTIONS", "json");
 *
 * Supported INDEXED_EXTRACTIONS values: "json", "csv", "tsv", "psv", "w3c"
 *
 * @param handle      Handle returned by tailin_create[_bytes]().
 * @param sourcetype  The props.conf stanza name (sourcetype).
 * @param key         Property key, e.g. "INDEXED_EXTRACTIONS".
 * @param value       Property value, e.g. "json".
 *
 * @return  0 on success, -1 on error (see tailin_last_error()).
 */
int tailin_set_props(TailinHandle* handle,
                     const char*   sourcetype,
                     const char*   key,
                     const char*   value);

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
