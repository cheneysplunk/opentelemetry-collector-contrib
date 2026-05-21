/**
 * tcpout_cabi.h
 *
 * Plain C interface to the Splunk TCP-output (S2S) library.
 * This header is the ONLY thing callers need — no Splunk headers required.
 *
 * Usage (C / C++):
 *   #include "tcpout_cabi.h"
 *   TcpoutHandle* h = tcpout_create_group("prod", NULL);
 *   tcpout_send(h, "hello world", 11, NULL, NULL, NULL, NULL);
 *   tcpout_destroy(h, 3);
 *
 * Usage (Go via CGo):
 *   // #cgo LDFLAGS: -L. -ltcpout_cabi -lstdc++ -ldl
 *   // #include "tcpout_cabi.h"
 *   import "C"
 *   h := C.tcpout_create_group(group, nil)
 *   C.tcpout_send(h, data, length, nil, nil, nil, nil)
 *   C.tcpout_destroy(h, 3)
 */

#ifndef TCPOUT_CABI_H
#define TCPOUT_CABI_H

#include <stddef.h>   /* size_t */

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque session handle returned by tcpout_create(). */
typedef struct TcpoutHandle TcpoutHandle;

/**
 * tcpout_create() — initialise the TCP output processor.
 *
 * @param host        Destination indexer hostname or IP (e.g. "10.0.0.5").
 * @param port        Destination port (usually 9997).
 * @param index       Default Splunk index name, e.g. "main".  Pass NULL
 *                    to leave unset (the receiving indexer will use its
 *                    default index).
 *
 * @return  Opaque handle on success, NULL on failure.
 *          Call tcpout_last_error() for a human-readable reason.
 */
TcpoutHandle* tcpout_create(const char* host, int port, const char* index);

/**
 * tcpout_create_group() — initialise TCP output from an existing outputs.conf
 * group.
 *
 * Does not write any in-memory outputs.conf. The native TcpOutputGroups
 * implementation reads the already-loaded merged outputs.conf cache and sends
 * through @p output_group by stamping _TCP_ROUTING on each event.
 *
 * @param output_group Bare tcpout group name, e.g. "prod" for [tcpout:prod].
 * @param index        Default Splunk index name, or NULL to leave unset.
 *
 * @return  Opaque handle on success, NULL on failure.
 *          Call tcpout_last_error() for a human-readable reason.
 */
TcpoutHandle* tcpout_create_group(const char* output_group,
                                  const char* index);

/**
 * tcpout_send() — send one raw event.
 *
 * All metadata fields are optional; pass NULL to omit.
 *
 * @param handle      Handle returned by tcpout_create().
 * @param data        Raw event bytes (need not be NUL-terminated).
 * @param len         Length of @p data in bytes.
 * @param source      Value for the "source" field, e.g. "/var/log/app.log".
 * @param sourcetype  Value for the "sourcetype" field, e.g. "_json".
 * @param host_field  Value for the "host" field, e.g. "myserver".
 *                    Defaults to the local hostname if NULL.
 * @param index       Override the index for this event only (or NULL to
 *                    use the default set in tcpout_create()).
 *
 * @return  0 on success, -1 on error (see tcpout_last_error()).
 */
int tcpout_send(TcpoutHandle* handle,
                const char*   data,
                size_t        len,
                const char*   source,
                const char*   sourcetype,
                const char*   host_field,
                const char*   index);

/**
 * tcpout_send_to_group() — send one raw event to a specific tcpout group.
 *
 * Sets Splunk's _TCP_ROUTING metadata before enqueueing the event.
 * @param output_group Bare tcpout group name. NULL/"" uses the handle's
 *                     default output group, or outputs.conf defaultGroup if
 *                     the handle has no default group.
 */
int tcpout_send_to_group(TcpoutHandle* handle,
                         const char*   output_group,
                         const char*   data,
                         size_t        len,
                         const char*   source,
                         const char*   sourcetype,
                         const char*   host_field,
                         const char*   index);

/**
 * tcpout_destroy() — flush queued events and shut down.
 *
 * @param handle      Handle to destroy.
 * @param drain_secs  Seconds to wait for the send queue to drain before
 *                    forcing shutdown.  Pass 3 if unsure.
 */
void tcpout_destroy(TcpoutHandle* handle, int drain_secs);

/**
 * tcpout_last_error() — human-readable description of the last error.
 *
 * The returned pointer is valid until the next call to any tcpout_*
 * function from the same thread.
 */
const char* tcpout_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* TCPOUT_CABI_H */
