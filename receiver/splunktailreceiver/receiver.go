// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

/*
// tailin_cabi.h is vendored from main/src/input/tail_lib/tailin_cabi.h.
// Provide its include path at build time via:
//   CGO_CFLAGS="-I/path/to/tail_lib"
// and the library path via:
//   CGO_LDFLAGS="-L/path/to/tail_lib -L/path/to/splunk_home/lib \
//                -Wl,-rpath,/path/to/tail_lib \
//                -Wl,-rpath,/path/to/splunk_home/lib"

#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -ltailinput_cabi -lstdc++ -ldl -lpthread

#include "tailin_cabi.h"
#include <stdlib.h>

// Forward declaration for the Go-exported callback trampoline.
// Note: CGo exports drop 'const', so the declaration must use non-const char*.
extern void goTailinEventCallback(
	char* data, size_t len,
	char* source, char* sourcetype,
	char* host, time_t event_time, void* userdata);
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// -----------------------------------------------------------------------------
// Global callback slot
//
// tailin_create() is a process-singleton: the C library bootstraps global
// singletons on the first call and reuses them on subsequent calls.
// Because CGo does not allow passing Go pointers containing Go pointers as
// userdata, we store the active receiver in a global and use a fixed sentinel
// as the userdata value (any non-nil value distinguishes "registered" from
// "not registered").
// -----------------------------------------------------------------------------

var (
	globalReceiverMu sync.RWMutex
	globalReceiver   *splunktailReceiver
)

func setGlobalReceiver(r *splunktailReceiver) {
	globalReceiverMu.Lock()
	defer globalReceiverMu.Unlock()
	globalReceiver = r
}

func clearGlobalReceiver() {
	globalReceiverMu.Lock()
	defer globalReceiverMu.Unlock()
	globalReceiver = nil
}

// -----------------------------------------------------------------------------
// CGo callback trampoline — called from the parsing-pipeline C thread.
// -----------------------------------------------------------------------------

//export goTailinEventCallback
func goTailinEventCallback(
	data *C.char, dataLen C.size_t,
	source *C.char, sourcetype *C.char,
	host *C.char, eventTime C.time_t,
	_ unsafe.Pointer,
) {
	globalReceiverMu.RLock()
	r := globalReceiver
	globalReceiverMu.RUnlock()
	if r == nil {
		return
	}

	raw := C.GoStringN(data, C.int(dataLen))
	if len(raw) == 0 {
		return // sentinel flush event — skip
	}

	src := C.GoString(source)
	st := C.GoString(sourcetype)
	h := C.GoString(host)
	ts := time.Unix(int64(eventTime), 0)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", h)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("splunktail")
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr(raw)
	lr.Attributes().PutStr("splunk.source", src)
	lr.Attributes().PutStr("splunk.sourcetype", st)

	if err := r.nextConsumer.ConsumeLogs(r.ctx, ld); err != nil {
		r.logger.Warn("ConsumeLogs error", zap.Error(err))
	}
}

// -----------------------------------------------------------------------------
// Receiver implementation
// -----------------------------------------------------------------------------

type splunktailReceiver struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs

	handle *C.TailinHandle

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func newLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg *Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	return &splunktailReceiver{
		cfg:          cfg,
		logger:       params.Logger,
		nextConsumer: nextConsumer,
	}, nil
}

func (r *splunktailReceiver) Start(_ context.Context, _ component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle != nil {
		return nil // already started
	}

	if r.cfg.SplunkHome != "" {
		if err := os.Setenv("SPLUNK_HOME", r.cfg.SplunkHome); err != nil {
			return fmt.Errorf("splunktailreceiver: set SPLUNK_HOME: %w", err)
		}
	}

	// Build TailinConfig from our Go config.
	ccfg := C.tailin_default_config()
	if r.cfg.DefaultSourcetype != "" {
		cs := C.CString(r.cfg.DefaultSourcetype)
		defer C.free(unsafe.Pointer(cs))
		ccfg.default_sourcetype = cs
	}
	if r.cfg.DefaultIndex != "" {
		ci := C.CString(r.cfg.DefaultIndex)
		defer C.free(unsafe.Pointer(ci))
		ccfg.default_index = ci
	}
	if r.cfg.Host != "" {
		ch := C.CString(r.cfg.Host)
		defer C.free(unsafe.Pointer(ch))
		ccfg.host = ch
	}
	if r.cfg.FishbucketDir != "" {
		cfb := C.CString(r.cfg.FishbucketDir)
		defer C.free(unsafe.Pointer(cfb))
		ccfg.fishbucket_dir = cfb
	}

	// Register before create so the callback can find us immediately.
	r.ctx, r.cancel = context.WithCancel(context.Background())
	setGlobalReceiver(r)

	handle := C.tailin_create(
		C.tailin_event_cb(C.goTailinEventCallback),
		nil, // userdata unused — trampoline reads globalReceiver
		&ccfg,
	)
	if handle == nil {
		clearGlobalReceiver()
		r.cancel()
		return fmt.Errorf("splunktailreceiver: tailin_create failed: %s",
			C.GoString(C.tailin_last_error()))
	}

	// Register all monitors.
	for _, m := range r.cfg.Monitors {
		cglob := C.CString(m.Glob)
		defer C.free(unsafe.Pointer(cglob))

		var cst, cidx, chost *C.char
		if m.Sourcetype != "" {
			cst = C.CString(m.Sourcetype)
			defer C.free(unsafe.Pointer(cst))
		}
		if m.Index != "" {
			cidx = C.CString(m.Index)
			defer C.free(unsafe.Pointer(cidx))
		}
		if m.Host != "" {
			chost = C.CString(m.Host)
			defer C.free(unsafe.Pointer(chost))
		}

		if rc := C.tailin_add_monitor(handle, cglob, cst, cidx, chost); rc != 0 {
			C.tailin_destroy(handle)
			clearGlobalReceiver()
			r.cancel()
			return fmt.Errorf("splunktailreceiver: tailin_add_monitor(%s) failed: %s",
				m.Glob, C.GoString(C.tailin_last_error()))
		}
	}

	// Start the tail pipeline.
	if rc := C.tailin_start(handle); rc != 0 {
		C.tailin_destroy(handle)
		clearGlobalReceiver()
		r.cancel()
		return fmt.Errorf("splunktailreceiver: tailin_start failed: %s",
			C.GoString(C.tailin_last_error()))
	}

	r.handle = handle
	r.logger.Info("splunktail receiver started",
		zap.Int("monitors", len(r.cfg.Monitors)),
	)
	return nil
}

func (r *splunktailReceiver) Shutdown(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handle == nil {
		return nil
	}

	if r.cancel != nil {
		r.cancel()
	}

	handle := r.handle
	r.handle = nil

	// Clear global receiver before stop so in-flight callbacks see nil and drop events.
	clearGlobalReceiver()

	// Stop blocks until all threads exit, then destroy frees memory.
	C.tailin_stop(handle)
	C.tailin_destroy(handle)

	r.logger.Info("splunktail receiver shut down")
	return nil
}
