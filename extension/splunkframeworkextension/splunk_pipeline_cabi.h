/**
 * splunk_pipeline_cabi.h
 *
 * Generic plain-C interface to the Splunk pipeline framework.
 *
 * Instead of component-specific APIs (tailin_create, tcpout_create, …),
 * this interface accepts raw Splunk conf stanza text and drives any
 * Splunk component that can be expressed in inputs.conf / outputs.conf /
 * props.conf.  No new CABI file is needed when adding new component types.
 *
 * The conf stanzas are injected directly into the in-memory PropertyPages
 * via saveStanza() — no files are written to disk, no conf reload is
 * required, and splunkfw_init() does not need to be re-run.
 *
 * Model
 * -----
 *   Input pipelines  → event callback (push to caller)
 *   Output pipelines → splunk_pipeline_send() (pull from caller)
 *   Mixed pipelines  → both
 *
 * Usage (input)
 * -------------
 *   const char* inputs =
 *       "[monitor:///var/log/app/*.log]\n"
 *       "sourcetype = myapp\n"
 *       "index = main\n";
 *
 *   SplunkPipeline* p = splunk_pipeline_create(inputs, NULL, NULL, my_cb, ctx);
 *   splunk_pipeline_start(p);
 *   // … events arrive via my_cb …
 *   splunk_pipeline_stop(p);
 *   splunk_pipeline_destroy(p);
 *
 * Usage (output)
 * --------------
 *   const char* outputs =
 *       "[tcpout]\ndefaultGroup = idx\n"
 *       "[tcpout:idx]\nserver = 10.0.0.1:9997\n";
 *
 *   SplunkPipeline* p = splunk_pipeline_create(NULL, outputs, NULL, NULL, NULL);
 *   splunk_pipeline_start(p);
 *   splunk_pipeline_send(p, data, len, source, sourcetype, host, index);
 *   splunk_pipeline_stop(p);
 *   splunk_pipeline_destroy(p);
 *
 * Threading
 * ---------
 * splunk_pipeline_create / start / stop / destroy must be called from
 * the same thread (or with external synchronisation).
 * splunk_pipeline_send() is thread-safe.
 * The event callback is invoked from Splunk's parsing-pipeline thread.
 * Do not call any splunk_pipeline_* function from within the callback.
 *
 * Lifecycle requirement
 * ---------------------
 * splunkfw_init() (from splunkfw_cabi.h) must have been called and
 * returned 0 before any splunk_pipeline_* call.
 */

#ifndef SPLUNK_PIPELINE_CABI_H
#define SPLUNK_PIPELINE_CABI_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque pipeline handle returned by splunk_pipeline_create(). */
typedef struct SplunkPipeline SplunkPipeline;

/**
 * splunk_event_cb — called once per fully line-broken event from an input.
 *
 * @param data        Event bytes (NUL-terminated for convenience).
 * @param len         Length of data in bytes (excluding NUL).
 * @param source      Absolute path or URI of the source (never NULL).
 * @param sourcetype  Sourcetype field value (never NULL).
 * @param host        Host field value (never NULL).
 * @param event_time  Unix timestamp (0 = not set).
 * @param userdata    The pointer passed to splunk_pipeline_create().
 */
typedef void (*splunk_event_cb)(
    const char* data,
    size_t      len,
    const char* source,
    const char* sourcetype,
    const char* host,
    time_t      event_time,
    void*       userdata
);

/**
 * splunk_bytes_cb — raw-bytes variant of splunk_event_cb.
 *
 * The @p data pointer is typed as const uint8_t* to make clear that the
 * buffer is an opaque byte slice of length @p len with no guaranteed NUL
 * terminator.  This avoids the internal NUL-termination copy incurred by
 * splunk_event_cb, making it the preferred callback for callers that treat
 * event data as []byte (e.g. Go via C.GoBytes).
 *
 * Use splunk_pipeline_create_bytes() to register this callback type.
 */
typedef void (*splunk_bytes_cb)(
    const uint8_t* data,
    size_t         len,
    const char*    source,
    const char*    sourcetype,
    const char*    host,
    time_t         event_time,
    void*          userdata
);

/**
 * splunk_pipeline_create() — create a pipeline from raw conf stanza text.
 *
 * All stanza arguments accept the same syntax as the corresponding
 * Splunk .conf files, including multiple stanzas separated by newlines.
 * Pass NULL or "" for any stanza that is not needed.
 *
 * @param inputs_conf   Raw inputs.conf stanza(s), e.g.:
 *                        "[monitor:///var/log/*.log]\nsourcetype=myapp\n"
 *                        "[udp://514]\nsourcetype=syslog\n"
 * @param outputs_conf  Raw outputs.conf stanza(s), e.g.:
 *                        "[tcpout]\ndefaultGroup=grp\n"
 *                        "[tcpout:grp]\nserver=10.0.0.1:9997\n"
 * @param props_conf    Raw props.conf stanza(s), or NULL for defaults.
 * @param cb            Event callback for input events; NULL for output-only.
 * @param userdata      Forwarded to every cb invocation.
 *
 * @return  Opaque handle on success, NULL on failure.
 *          Call splunk_pipeline_last_error() for details.
 */
SplunkPipeline* splunk_pipeline_create(
    const char*    inputs_conf,
    const char*    outputs_conf,
    const char*    props_conf,
    splunk_event_cb cb,
    void*          userdata
);

/**
 * splunk_pipeline_create_bytes() — raw-bytes variant of splunk_pipeline_create().
 *
 * Identical to splunk_pipeline_create() but registers a splunk_bytes_cb.
 * Event bytes are passed directly without a NUL-termination copy, making
 * this the preferred path for Go callers that use C.GoBytes().
 */
SplunkPipeline* splunk_pipeline_create_bytes(
    const char*    inputs_conf,
    const char*    outputs_conf,
    const char*    props_conf,
    splunk_bytes_cb cb,
    void*          userdata
);

/**
 * splunk_pipeline_create_output_group() — create an output pipeline from an
 * existing outputs.conf tcpout group.
 *
 * Does not inject or write outputs.conf. The native TcpOutputGroups code reads
 * the already-loaded merged outputs.conf cache.
 *
 * @param output_group Bare tcpout group name, e.g. "prod" for [tcpout:prod].
 * @param default_index Default Splunk index name, or NULL to leave unset.
 */
SplunkPipeline* splunk_pipeline_create_output_group(
    const char* output_group,
    const char* default_index
);

/**
 * splunk_pipeline_start() — start all inputs and outputs.
 *
 * For input pipelines, events begin arriving via the callback immediately.
 * For output pipelines, the TCP/UDP connection is established.
 *
 * @return 0 on success, non-zero on failure.
 */
int splunk_pipeline_start(SplunkPipeline* p);

/**
 * splunk_pipeline_send() — send one event through an output pipeline.
 *
 * Thread-safe.  Blocks if the output queue is full.
 *
 * @param data        Raw event bytes.
 * @param len         Length of data.
 * @param source      Source field value; NULL → default from outputs.conf.
 * @param sourcetype  Sourcetype field value; NULL → default.
 * @param host        Host field value; NULL → local hostname.
 * @param index       Target index; NULL → default from outputs.conf.
 *
 * @return 0 on success, non-zero on failure.
 */
int splunk_pipeline_send(
    SplunkPipeline* p,
    const char*     data,
    size_t          len,
    const char*     source,
    const char*     sourcetype,
    const char*     host,
    const char*     index
);

/**
 * splunk_pipeline_send_to_group() — send one event to a specific tcpout group.
 *
 * Sets Splunk's _TCP_ROUTING metadata to @p output_group. Pass NULL/"" to use
 * the pipeline's default output group or outputs.conf defaultGroup.
 */
int splunk_pipeline_send_to_group(
    SplunkPipeline* p,
    const char*     output_group,
    const char*     data,
    size_t          len,
    const char*     source,
    const char*     sourcetype,
    const char*     host,
    const char*     index
);

/**
 * splunk_pipeline_stop() — stop all inputs and flush all outputs.
 *
 * For input pipelines: stops the parsing thread; no more callbacks after return.
 * For output pipelines: drains the send queue (waits up to drain_secs seconds).
 *
 * splunk_pipeline_destroy() must be called after stop().
 */
void splunk_pipeline_stop(SplunkPipeline* p, int drain_secs);

/**
 * splunk_pipeline_destroy() — free all resources.
 *
 * Must be called after splunk_pipeline_stop().
 */
void splunk_pipeline_destroy(SplunkPipeline* p);

/**
 * splunk_pipeline_last_error() — human-readable description of last error.
 *
 * The returned pointer is valid until the next call to any
 * splunk_pipeline_* function from the same thread.
 */
const char* splunk_pipeline_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* SPLUNK_PIPELINE_CABI_H */
