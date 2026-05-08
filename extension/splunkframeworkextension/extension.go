// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkframeworkextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"

/*
// All three CABI headers are vendored from the Splunk source tree:
//   splunkfw_cabi.h  ←  main/src/framework_cabi/
//   tailin_cabi.h    ←  main/src/input/tail_lib/
//   tcpout_cabi.h    ←  main/src/output/tcpout_lib/
//
// All symbols live in one unified library: libsplunk_cabi.so
// Build requirements:
//   CGO_LDFLAGS="-L/path/to/framework_cabi -lsplunk_cabi \
//                -Wl,-rpath,/path/to/framework_cabi \
//                -Wl,-rpath,/path/to/splunk_home/lib \
//                -lstdc++ -ldl -lpthread"
//
// All three headers are vendored in this directory and found automatically
// via `#cgo CFLAGS: -I${SRCDIR}`.

#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -lsplunk_cabi -lstdc++ -ldl -lpthread

#include "splunkfw_cabi.h"
#include "tailin_cabi.h"
#include "tcpout_cabi.h"
#include <stdint.h>
#include <stdlib.h>

// Forward declaration for the Go-exported callback.
// Note: CGo //export drops 'const', so the declaration must use non-const char*.
extern void goTailinTrampoline(char* data, size_t len,
                               char* source, char* sourcetype,
                               char* host, time_t event_time,
                               void* userdata);
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

// ── callback registry ────────────────────────────────────────────────────────
// Each TailInput gets a unique int32 ID.  The C trampoline receives this ID
// as userdata, looks up the channel, and sends the event.

var (
	tailinNextID  int32 // atomic
	tailinRegistry sync.Map // int32 → chan splunkapi.TailEvent
)

//export goTailinTrampoline
func goTailinTrampoline(
	data *C.char, dataLen C.size_t,
	source *C.char, sourcetype *C.char,
	host *C.char, eventTime C.time_t,
	userdata unsafe.Pointer,
) {
	id := int32(uintptr(userdata))
	v, ok := tailinRegistry.Load(id)
	if !ok {
		return
	}
	ch := v.(chan splunkapi.TailEvent)

	raw := C.GoStringN(data, C.int(dataLen))
	if len(raw) == 0 {
		return // sentinel flush event
	}
	ev := splunkapi.TailEvent{
		Body:       raw,
		Source:     C.GoString(source),
		Sourcetype: C.GoString(sourcetype),
		Host:       C.GoString(host),
		Time:       time.Unix(int64(eventTime), 0),
	}
	// Non-blocking send: if the consumer is slow we drop rather than block
	// the C parsing thread.
	select {
	case ch <- ev:
	default:
	}
}

// ── splunkFrameworkExtension ─────────────────────────────────────────────────

// splunkFrameworkExtension is the Linux CGo implementation.
// It implements both component.Component and splunkapi.SplunkFramework.
type splunkFrameworkExtension struct {
	cfg    *Config
	logger *zap.Logger
}

func newSplunkFrameworkExtension(set extension.Settings, cfg *Config) *splunkFrameworkExtension {
	return &splunkFrameworkExtension{cfg: cfg, logger: set.Logger}
}

// Start initialises SplunkMainThread via splunkfw_init().
// All tailin_* and tcpout_* calls are safe only after Start returns nil.
func (e *splunkFrameworkExtension) Start(_ context.Context, _ component.Host) error {
	if e.cfg.SplunkHome == "" {
		return fmt.Errorf("splunkframeworkextension: splunk_home must not be empty")
	}

	cHome := C.CString(e.cfg.SplunkHome)
	defer C.free(unsafe.Pointer(cHome))

	var cDB *C.char
	if e.cfg.SplunkDB != "" {
		cDB = C.CString(e.cfg.SplunkDB)
		defer C.free(unsafe.Pointer(cDB))
	}

	if rc := C.splunkfw_init(cHome, cDB); rc != 0 {
		return fmt.Errorf("splunkframeworkextension: splunkfw_init failed: %s",
			C.GoString(C.splunkfw_last_error()))
	}

	e.logger.Info("Splunk framework initialised",
		zap.String("splunk_home", e.cfg.SplunkHome),
	)
	return nil
}

// Shutdown stops SplunkMainThread. Must be called after all TailInputs and
// TcpOutputs have been destroyed by their owning components.
func (e *splunkFrameworkExtension) Shutdown(_ context.Context) error {
	C.splunkfw_shutdown()
	e.logger.Info("Splunk framework shut down")
	return nil
}

// ── splunkapi.SplunkFramework implementation ─────────────────────────────────

// NewTailInput creates a Splunk tail-input session.
func (e *splunkFrameworkExtension) NewTailInput(cfg splunkapi.TailConfig) (splunkapi.TailInput, error) {
	ccfg := C.tailin_default_config()
	if cfg.DefaultSourcetype != "" {
		cs := C.CString(cfg.DefaultSourcetype)
		defer C.free(unsafe.Pointer(cs))
		ccfg.default_sourcetype = cs
	}
	if cfg.DefaultIndex != "" {
		ci := C.CString(cfg.DefaultIndex)
		defer C.free(unsafe.Pointer(ci))
		ccfg.default_index = ci
	}
	if cfg.Host != "" {
		ch := C.CString(cfg.Host)
		defer C.free(unsafe.Pointer(ch))
		ccfg.host = ch
	}
	if cfg.FishbucketDir != "" {
		cfb := C.CString(cfg.FishbucketDir)
		defer C.free(unsafe.Pointer(cfb))
		ccfg.fishbucket_dir = cfb
	}

	// Allocate a registry ID and channel before creating the handle so the
	// trampoline can always find the channel from the first callback.
	id := atomic.AddInt32(&tailinNextID, 1)
	ch := make(chan splunkapi.TailEvent, 1024)
	tailinRegistry.Store(id, ch)

	handle := C.tailin_create(
		C.tailin_event_cb(C.goTailinTrampoline),
		unsafe.Pointer(uintptr(id)), // userdata = id
		&ccfg,
	)
	if handle == nil {
		tailinRegistry.Delete(id)
		close(ch)
		return nil, fmt.Errorf("tailin_create failed: %s", C.GoString(C.tailin_last_error()))
	}

	return &cTailInput{handle: handle, id: id, ch: ch}, nil
}

// NewTcpOutput creates an S2S TCP output session.
func (e *splunkFrameworkExtension) NewTcpOutput(host string, port int, index string) (splunkapi.TcpOutput, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var cIndex *C.char
	if index != "" {
		cIndex = C.CString(index)
		defer C.free(unsafe.Pointer(cIndex))
	}

	handle := C.tcpout_create(cHost, C.int(port), cIndex)
	if handle == nil {
		return nil, fmt.Errorf("tcpout_create(%s:%d) failed: %s",
			host, port, C.GoString(C.tcpout_last_error()))
	}
	return &cTcpOutput{handle: handle}, nil
}

// ── cTailInput ───────────────────────────────────────────────────────────────

type cTailInput struct {
	handle  *C.TailinHandle
	id      int32
	ch      chan splunkapi.TailEvent
	stopped atomic.Bool
}

func (t *cTailInput) AddMonitor(cfg splunkapi.MonitorConfig) error {
	cglob := C.CString(cfg.Glob)
	defer C.free(unsafe.Pointer(cglob))

	var cst, cidx, chost *C.char
	if cfg.Sourcetype != "" {
		cst = C.CString(cfg.Sourcetype)
		defer C.free(unsafe.Pointer(cst))
	}
	if cfg.Index != "" {
		cidx = C.CString(cfg.Index)
		defer C.free(unsafe.Pointer(cidx))
	}
	if cfg.Host != "" {
		chost = C.CString(cfg.Host)
		defer C.free(unsafe.Pointer(chost))
	}

	if rc := C.tailin_add_monitor(t.handle, cglob, cst, cidx, chost); rc != 0 {
		return fmt.Errorf("tailin_add_monitor(%s) failed: %s",
			cfg.Glob, C.GoString(C.tailin_last_error()))
	}
	return nil
}

func (t *cTailInput) Start() error {
	if rc := C.tailin_start(t.handle); rc != 0 {
		return fmt.Errorf("tailin_start failed: %s", C.GoString(C.tailin_last_error()))
	}
	return nil
}

func (t *cTailInput) Events() <-chan splunkapi.TailEvent { return t.ch }

func (t *cTailInput) Stop() {
	if t.stopped.CompareAndSwap(false, true) {
		tailinRegistry.Delete(t.id) // no more trampoline deliveries
		C.tailin_stop(t.handle)
		close(t.ch)
	}
}

func (t *cTailInput) Destroy() {
	C.tailin_destroy(t.handle)
}

// ── cTcpOutput ───────────────────────────────────────────────────────────────

type cTcpOutput struct {
	handle *C.TcpoutHandle
	mu     sync.Mutex
}

func (o *cTcpOutput) Send(body []byte, source, sourcetype, host, index string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.handle == nil {
		return fmt.Errorf("tcpout: already destroyed")
	}

	cData := C.CString(string(body))
	defer C.free(unsafe.Pointer(cData))
	cSource := C.CString(source)
	defer C.free(unsafe.Pointer(cSource))
	cSourcetype := C.CString(sourcetype)
	defer C.free(unsafe.Pointer(cSourcetype))
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var cIndex *C.char
	if index != "" {
		cIndex = C.CString(index)
		defer C.free(unsafe.Pointer(cIndex))
	}

	rc := C.tcpout_send(o.handle,
		cData, C.size_t(len(body)),
		cSource, cSourcetype, cHost, cIndex)
	if rc != 0 {
		return fmt.Errorf("tcpout_send: %s", C.GoString(C.tcpout_last_error()))
	}
	return nil
}

func (o *cTcpOutput) Destroy(drainSeconds int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.handle != nil {
		C.tcpout_destroy(o.handle, C.int(drainSeconds))
		o.handle = nil
	}
}
