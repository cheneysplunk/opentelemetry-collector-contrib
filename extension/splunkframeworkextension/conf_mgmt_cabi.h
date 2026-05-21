/**
 * conf_mgmt_cabi.h
 *
 * Plain C interface to the Splunk configuration management library.
 * This header is the ONLY thing callers need — no Splunk headers required.
 *
 * Features:
 *   1. Conf read/merge   — read and merge layered Splunk .conf files
 *   2. Auth / authz      — authentication (authentication.conf) and
 *                          authorization (authorize.conf) via the existing
 *                          Splunk UserManager / SessionManager framework
 *   3. Management port   — native HTTPRestDispatcher/AdminManager REST path
 *                          for auth and conf endpoints
 *
 * Usage (C / C++):
 *   #include "conf_mgmt_cabi.h"
 *
 *   ConfMgmtHandle* h = confmgmt_create("/opt/splunkforwarder");
 *
 *   // read a conf value
 *   const char* val = confmgmt_get(h, "server", "general", "serverName");
 *
 *   // authenticate a user
 *   ConfMgmtSession* s = confmgmt_auth_login(h, "admin", "changeme");
 *
 *   // start mgmt port
 *   confmgmt_mgmt_start(h, 8089);
 *
 *   confmgmt_auth_logout(h, s);
 *   confmgmt_mgmt_stop(h);
 *   confmgmt_destroy(h);
 *
 * Usage (Go via CGo):
 *   // #cgo LDFLAGS: -L. -lconf_mgmt_cabi -lstdc++ -ldl
 *   // #include "conf_mgmt_cabi.h"
 *   import "C"
 *   h := C.confmgmt_create(splunkHome)
 *   defer C.confmgmt_destroy(h)
 */

#ifndef CONF_MGMT_CABI_H
#define CONF_MGMT_CABI_H

#include <stddef.h>   /* size_t */

#ifdef __cplusplus
extern "C" {
#endif

/* =====================================================================
 * Opaque handles
 * ===================================================================== */

/** Opaque handle returned by confmgmt_create(). */
typedef struct ConfMgmtHandle  ConfMgmtHandle;

/** Opaque session token returned by confmgmt_auth_login(). */
typedef struct ConfMgmtSession ConfMgmtSession;

/* =====================================================================
 * 1. Lifecycle
 * ===================================================================== */

/**
 * confmgmt_create() — initialise the conf management subsystem.
 *
 * Bootstraps the Splunk framework (PropertyPages, BundlesSetup, etc.)
 * rooted at @p splunk_home.  Reads and merges all layered .conf files
 * under $SPLUNK_HOME/etc/.
 *
 * @param splunk_home  Path to $SPLUNK_HOME (e.g. "/opt/splunkforwarder").
 *                     Must contain etc/system/ with default .conf files.
 * @return  Opaque handle on success, NULL on failure.
 *          Call confmgmt_last_error() for a human-readable reason.
 */
ConfMgmtHandle* confmgmt_create(const char* splunk_home);

/**
 * confmgmt_destroy() — shut down and free all resources.
 *
 * Stops the management HTTP server (if running), invalidates all
 * sessions, and releases the framework state.
 */
void confmgmt_destroy(ConfMgmtHandle* handle);

/**
 * confmgmt_last_error() — human-readable description of the last error.
 *
 * Thread-local; valid until the next confmgmt_* call on the same thread.
 */
const char* confmgmt_last_error(void);

/* =====================================================================
 * 2. Conf read / merge
 * ===================================================================== */

/**
 * confmgmt_get() — read a single conf key value.
 *
 * Reads the merged (default + local + app-layered) value for
 * @p key under @p stanza in @p conf_name.  For example:
 *
 *   confmgmt_get(h, "server", "general", "serverName")
 *
 * reads [general] serverName from server.conf (merged).
 *
 * @param handle     Handle returned by confmgmt_create().
 * @param conf_name  Conf file basename without ".conf", e.g. "server".
 * @param stanza     Stanza name, e.g. "general" or "default".
 * @param key        Key name within the stanza.
 *
 * @return  The merged value string (owned by the conf system — do NOT
 *          free), or NULL if the key does not exist.
 */
const char* confmgmt_get(ConfMgmtHandle* handle,
                         const char*     conf_name,
                         const char*     stanza,
                         const char*     key);

/**
 * confmgmt_set() — write a conf key value to the local layer.
 *
 * Writes @p value for @p key under @p stanza in the local layer of
 * @p conf_name.  Creates the stanza if it does not exist.
 *
 * @return  0 on success, -1 on error (see confmgmt_last_error()).
 */
int confmgmt_set(ConfMgmtHandle* handle,
                 const char*     conf_name,
                 const char*     stanza,
                 const char*     key,
                 const char*     value);

/**
 * confmgmt_get_stanza() — list all key=value pairs in a stanza.
 *
 * Writes newline-separated "key=value" pairs into @p buf.
 *
 * @param handle     Handle from confmgmt_create().
 * @param conf_name  Conf file basename without ".conf".
 * @param stanza     Stanza name.
 * @param buf        Caller-allocated output buffer.
 * @param buf_len    Size of @p buf in bytes.
 *
 * @return  Number of bytes written (excluding NUL), or -1 on error.
 *          If the output was truncated, returns the total size needed.
 */
int confmgmt_get_stanza(ConfMgmtHandle* handle,
                        const char*     conf_name,
                        const char*     stanza,
                        char*           buf,
                        size_t          buf_len);

/**
 * confmgmt_list_stanzas() — list all stanza names in a conf file.
 *
 * Writes newline-separated stanza names into @p buf.
 *
 * @return  Number of bytes written (excluding NUL), or -1 on error.
 */
int confmgmt_list_stanzas(ConfMgmtHandle* handle,
                          const char*     conf_name,
                          char*           buf,
                          size_t          buf_len);

/**
 * confmgmt_delete_stanza() — delete an entire stanza from the local layer.
 *
 * @return  0 on success, -1 on error.
 */
int confmgmt_delete_stanza(ConfMgmtHandle* handle,
                           const char*     conf_name,
                           const char*     stanza);

/**
 * confmgmt_delete_key() — delete a single key from a stanza in the local layer.
 *
 * @return  0 on success, -1 on error.
 */
int confmgmt_delete_key(ConfMgmtHandle* handle,
                        const char*     conf_name,
                        const char*     stanza,
                        const char*     key);

/**
 * confmgmt_reload() — re-read and re-merge a conf file from disk.
 *
 * @return  0 on success, -1 on error.
 */
int confmgmt_reload(ConfMgmtHandle* handle,
                    const char*     conf_name);

/* =====================================================================
 * 3. Authentication & authorisation
 * ===================================================================== */

/**
 * confmgmt_auth_login() — authenticate a user.
 *
 * Uses the authentication provider configured in authentication.conf
 * (Splunk built-in, LDAP, SAML, scripted, etc.) to validate
 * @p username / @p password and create a session.
 *
 * @return  Session handle on success, NULL on failure.
 */
ConfMgmtSession* confmgmt_auth_login(ConfMgmtHandle* handle,
                                     const char*     username,
                                     const char*     password);

/**
 * confmgmt_auth_logout() — invalidate a session.
 */
void confmgmt_auth_logout(ConfMgmtHandle* handle,
                          ConfMgmtSession* session);

/**
 * confmgmt_auth_check_capability() — check whether a session has a
 * specific capability as defined in authorize.conf.
 *
 * @param handle      Handle from confmgmt_create().
 * @param session     Session from confmgmt_auth_login().
 * @param capability  Capability name (e.g. "admin_all_objects").
 *
 * @return  1 if the user has the capability, 0 if not, -1 on error.
 */
int confmgmt_auth_check_capability(ConfMgmtHandle*  handle,
                                   ConfMgmtSession* session,
                                   const char*      capability);

/**
 * confmgmt_auth_get_roles() — get the roles for a session.
 *
 * Writes newline-separated role names into @p buf.
 *
 * @return  Number of bytes written (excluding NUL), or -1 on error.
 */
int confmgmt_auth_get_roles(ConfMgmtHandle*  handle,
                            ConfMgmtSession* session,
                            char*            buf,
                            size_t           buf_len);

/* =====================================================================
 * 4. Management port (HTTP server for REST auth/conf API)
 * ===================================================================== */

/**
 * confmgmt_mgmt_start() — start the management HTTP server.
 *
 * Binds to @p port and serves native Splunk REST endpoints for
 * authentication and conf management (services/auth/login,
 * services/configs/conf-*). The HTTP/HTTPS scheme follows server.conf.
 *
 * The server runs on a background thread.
 *
 * @param handle  Handle from confmgmt_create().
 * @param port    TCP port to listen on (e.g. 8089).
 *
 * @return  0 on success, -1 on error.
 */
int confmgmt_mgmt_start(ConfMgmtHandle* handle, int port);

/**
 * confmgmt_mgmt_stop() — stop the management HTTP server.
 */
void confmgmt_mgmt_stop(ConfMgmtHandle* handle);

/**
 * confmgmt_mgmt_is_running() — check whether the management server is up.
 *
 * @return  1 if running, 0 if not.
 */
int confmgmt_mgmt_is_running(ConfMgmtHandle* handle);

#ifdef __cplusplus
}
#endif

#endif /* CONF_MGMT_CABI_H */
