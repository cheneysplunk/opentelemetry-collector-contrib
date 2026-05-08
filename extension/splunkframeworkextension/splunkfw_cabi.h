/**
 * splunkfw_cabi.h
 *
 * Plain C interface for the Splunk framework bootstrap.
 *
 * This replaces the per-library bootstrap that was previously embedded
 * in tailin_cabi.cpp and tcpout_cabi.cpp.  It must be called once per
 * process — before any tailin_* or tcpout_* calls — to initialise the
 * shared SplunkMainThread / EventLoop singleton.
 *
 * In the OTel Collector this is done by splunkframeworkextension, which
 * calls splunkfw_init() in Start() and splunkfw_shutdown() in Shutdown().
 * Both splunktailreceiver and splunktcpoutexporter then call into the
 * already-running framework via their existing tailin_* / tcpout_* CABIs.
 *
 * This header (and splunkfw_cabi.cpp) must be compiled into the SAME
 * shared library as tailin_cabi.cpp and tcpout_cabi.cpp so that all
 * three compilation units share one copy of SplunkMainThread::_instance.
 * The resulting unified library is libsplunk_cabi.so.
 *
 * Usage:
 *   if (splunkfw_init("/opt/splunk", NULL) != 0) {
 *       fprintf(stderr, "framework init failed: %s\n", splunkfw_last_error());
 *   }
 *   // ... use tailin_* and tcpout_* ...
 *   splunkfw_shutdown();
 */

#ifndef SPLUNKFW_CABI_H
#define SPLUNKFW_CABI_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * splunkfw_init() — bootstrap the Splunk framework.
 *
 * Initialises: Logger, GlobalConfSettings, LoaderInfo, BulletinBoardManager,
 * FileTracker (fishbucket), EventLoop::enableFastPoll(), PluginProcessor
 * factory lookup, and starts a SplunkMainThread on a dedicated OS thread.
 *
 * Idempotent — guarded by std::call_once.  Safe to call multiple times;
 * the bootstrap runs exactly once per process.
 *
 * @param splunk_home  Value for $SPLUNK_HOME, e.g. "/opt/splunk".
 *                     Must not be NULL or empty.
 * @param splunk_db    Value for $SPLUNK_DB.  Pass NULL or "" to default to
 *                     $SPLUNK_HOME/var/lib/splunk.
 *
 * @return  0 on success, -1 on failure (call splunkfw_last_error()).
 */
int splunkfw_init(const char* splunk_home, const char* splunk_db);

/**
 * splunkfw_is_running() — non-zero if splunkfw_init() has completed
 * successfully and the SplunkMainThread EventLoop is still running.
 *
 * Used by tailin_cabi.cpp and tcpout_cabi.cpp to skip their own
 * per-library bootstrap paths when the extension has already initialised
 * the framework.
 */
int splunkfw_is_running(void);

/**
 * splunkfw_shutdown() — stop the SplunkMainThread and join its thread.
 *
 * Must be called after all tailin_* and tcpout_* handles have been
 * destroyed.  After this call the framework is no longer operational.
 */
void splunkfw_shutdown(void);

/**
 * splunkfw_last_error() — human-readable description of the last error.
 *
 * The returned pointer is valid until the next splunkfw_* call on the
 * same thread.
 */
const char* splunkfw_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* SPLUNKFW_CABI_H */
