/**
 * exec_processor_cabi.h
 *
 * Plain C interface to Splunk's scripted-input ExecProcessor.
 * Callers see only this C ABI; all Splunk C++ headers stay behind the
 * boundary in exec_processor_cabi.cpp.
 */

#ifndef EXEC_PROCESSOR_CABI_H
#define EXEC_PROCESSOR_CABI_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ExecProcHandle ExecProcHandle;

typedef void (*execproc_bytes_cb)(
    const uint8_t* data,
    size_t         len,
    const char*    source,
    const char*    sourcetype,
    const char*    host,
    time_t         event_time,
    void*          userdata
);

typedef struct ExecProcKV {
    const char* key;
    const char* value;
} ExecProcKV;

typedef struct ExecProcConfig {
    const char* default_sourcetype;
    const char* default_index;
    const char* host;
} ExecProcConfig;

ExecProcConfig execproc_default_config(void);

ExecProcHandle* execproc_create_bytes(execproc_bytes_cb cb,
                                      void* userdata,
                                      const ExecProcConfig* cfg);

int execproc_add_script(ExecProcHandle* handle,
                        const char* command,
                        const char* interval,
                        const char* sourcetype,
                        const char* index,
                        const char* host,
                        const char* source,
                        int start_by_shell);

int execproc_add_stanza(ExecProcHandle* handle,
                        const char* stanza,
                        const ExecProcKV* kvs,
                        size_t kv_count);

int execproc_set_props(ExecProcHandle* handle,
                       const char* sourcetype,
                       const char* key,
                       const char* value);

int execproc_start(ExecProcHandle* handle);

void execproc_stop(ExecProcHandle* handle);

void execproc_destroy(ExecProcHandle* handle);

const char* execproc_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* EXEC_PROCESSOR_CABI_H */
