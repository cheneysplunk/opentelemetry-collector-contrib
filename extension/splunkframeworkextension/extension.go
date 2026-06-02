// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkframeworkextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"

/*
// All CABI headers are vendored from the Splunk source tree and found
// automatically via `#cgo CFLAGS: -I${SRCDIR}`.
//
// Build requirements (only CGO_LDFLAGS is needed):
//   CGO_LDFLAGS="-L/path/to/framework_cabi -lsplunk_cabi \
//                -Wl,-rpath,/path/to/framework_cabi \
//                -Wl,-rpath,/path/to/splunk_home/lib \
//                -lstdc++ -ldl -lpthread"

#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -lsplunk_cabi -lstdc++ -ldl -lpthread

#include "splunkfw_cabi.h"
#include "splunk_pipeline_cabi.h"
#include <stdlib.h>
#include <stdint.h>

// Forward declaration for the Go-exported event callback.
// CGo //export drops 'const', so use non-const char* here.
extern void goPipelineEventCallback(char* data, size_t len,
                                    char* source, char* sourcetype,
                                    char* host, time_t event_time,
                                    void* userdata);

// Forward declaration for the raw-bytes callback (uint8_t* data).
extern void goPipelineBytesCallback(uint8_t* data, size_t len,
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

// ── event registry ────────────────────────────────────────────────────────────
// Each Pipeline with an input side gets a unique int32 ID stored as the C
// userdata pointer. The C callback thread calls goPipelineEventCallback which
// looks up the channel by ID and sends the event — no Go pointer passed to C.

var (
	pipelineNextID   atomic.Int32
	pipelineRegistry sync.Map // int32 → chan splunkapi.Event
)

//export goPipelineEventCallback
func goPipelineEventCallback(
	data *C.char, dataLen C.size_t,
	source *C.char, sourcetype *C.char,
	host *C.char, eventTime C.time_t,
	userdata unsafe.Pointer,
) {
	id := int32(uintptr(userdata))
	v, ok := pipelineRegistry.Load(id)
	if !ok {
		return
	}
	ch := v.(chan splunkapi.Event)

	raw := C.GoStringN(data, C.int(dataLen))
	if len(raw) == 0 {
		return // sentinel flush event — skip
	}
	ev := splunkapi.Event{
		Body:       raw,
		Source:     C.GoString(source),
		Sourcetype: C.GoString(sourcetype),
		Host:       C.GoString(host),
		Time:       time.Unix(int64(eventTime), 0),
	}
	// Non-blocking: drop if the consumer goroutine is behind rather than
	// stalling the C parsing thread.
	select {
	case ch <- ev:
	default:
	}
}

//export goPipelineBytesCallback
func goPipelineBytesCallback(
	data *C.uint8_t, dataLen C.size_t,
	source *C.char, sourcetype *C.char,
	host *C.char, eventTime C.time_t,
	userdata unsafe.Pointer,
) {
	id := int32(uintptr(userdata))
	v, ok := pipelineRegistry.Load(id)
	if !ok {
		return
	}
	ch := v.(chan splunkapi.Event)

	if dataLen == 0 {
		return // sentinel flush event — skip
	}
	ev := splunkapi.Event{
		RawBody:    C.GoBytes(unsafe.Pointer(data), C.int(dataLen)),
		Source:     C.GoString(source),
		Sourcetype: C.GoString(sourcetype),
		Host:       C.GoString(host),
		Time:       time.Unix(int64(eventTime), 0),
	}
	select {
	case ch <- ev:
	default:
	}
}

// ── splunkFrameworkExtension ──────────────────────────────────────────────────

type splunkFrameworkExtension struct {
	cfg           *Config
	logger        *zap.Logger
	confMu        sync.Mutex
	confMgr       *cConfManager
	monitorMu     sync.Mutex
	monitorActive bool
}

func newSplunkFrameworkExtension(set extension.Settings, cfg *Config) *splunkFrameworkExtension {
	return &splunkFrameworkExtension{cfg: cfg, logger: set.Logger}
}

func (e *splunkFrameworkExtension) Start(_ context.Context, _ component.Host) error {
	if e.cfg.SplunkHome == "" {
		return fmt.Errorf("splunkframeworkextension: splunk_home must not be empty")
	}
	if e.cfg.ManagementPort < 0 || e.cfg.ManagementPort > 65535 {
		return fmt.Errorf("splunkframeworkextension: management_port must be between 0 and 65535")
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

	confManagerLoaded := false
	if e.cfg.ManagementPort > 0 {
		e.confMu.Lock()
		confMgr := e.confMgr
		if confMgr == nil {
			var err error
			confMgr, err = newCConfManager(e.cfg.SplunkHome)
			if err != nil {
				e.confMu.Unlock()
				C.splunkfw_shutdown()
				return fmt.Errorf("splunkframeworkextension: conf manager init failed: %w", err)
			}
			e.confMgr = confMgr
		}
		if err := confMgr.StartREST(e.cfg.ManagementPort); err != nil {
			e.confMgr = nil
			e.confMu.Unlock()
			confMgr.Close()
			C.splunkfw_shutdown()
			return fmt.Errorf("splunkframeworkextension: REST server init failed: %w", err)
		}
		confManagerLoaded = true
		e.confMu.Unlock()
	}

	fields := []zap.Field{
		zap.String("splunk_home", e.cfg.SplunkHome),
		zap.Bool("conf_manager_loaded", confManagerLoaded),
	}
	if e.cfg.ManagementPort > 0 {
		fields = append(fields, zap.Int("management_port", e.cfg.ManagementPort))
	}
	e.logger.Info("Splunk framework initialised", fields...)
	return nil
}

func (e *splunkFrameworkExtension) Shutdown(_ context.Context) error {
	e.confMu.Lock()
	confMgr := e.confMgr
	e.confMgr = nil
	e.confMu.Unlock()
	if confMgr != nil {
		confMgr.Close()
	}

	C.splunkfw_shutdown()
	e.logger.Info("Splunk framework shut down")
	return nil
}

// ── splunkapi.SplunkFramework ─────────────────────────────────────────────────

// NewPipeline creates a Splunk pipeline from raw conf stanza text.
// Either InputsConf or OutputsConf (or both) must be non-empty.
func (e *splunkFrameworkExtension) NewPipeline(cfg splunkapi.PipelineConfig) (splunkapi.Pipeline, error) {
	var cInputs, cOutputs, cProps *C.char

	if cfg.InputsConf != "" {
		cInputs = C.CString(cfg.InputsConf)
		defer C.free(unsafe.Pointer(cInputs))
	}
	if cfg.OutputsConf != "" {
		cOutputs = C.CString(cfg.OutputsConf)
		defer C.free(unsafe.Pointer(cOutputs))
	}
	if cfg.PropsConf != "" {
		cProps = C.CString(cfg.PropsConf)
		defer C.free(unsafe.Pointer(cProps))
	}

	// Allocate event channel and registry slot only for input pipelines.
	var id int32
	var ch chan splunkapi.Event
	var cb C.splunk_bytes_cb
	var udPtr unsafe.Pointer
	if cfg.InputsConf != "" {
		id = pipelineNextID.Add(1)
		ch = make(chan splunkapi.Event, 1024)
		pipelineRegistry.Store(id, ch)
		cb = C.splunk_bytes_cb(C.goPipelineBytesCallback)
		idWord := uintptr(id)
		udPtr = *(*unsafe.Pointer)(unsafe.Pointer(&idWord))
	}

	handle := C.splunk_pipeline_create_bytes(cInputs, cOutputs, cProps, cb, udPtr)
	if handle == nil {
		if ch != nil {
			pipelineRegistry.Delete(id)
			close(ch)
		}
		return nil, fmt.Errorf("splunk_pipeline_create failed: %s",
			C.GoString(C.splunk_pipeline_last_error()))
	}

	return &cPipeline{handle: handle, id: id, ch: ch}, nil
}

// NewOutputPipeline creates a tcpout pipeline from an existing outputs.conf
// group. The native C++ layer reads the merged outputs.conf cache; Go only
// passes the bare group name through the CABI.
func (e *splunkFrameworkExtension) NewOutputPipeline(outputGroup, defaultIndex string) (splunkapi.Pipeline, error) {
	if outputGroup == "" {
		return nil, fmt.Errorf("splunkframeworkextension: output_group must not be empty")
	}

	cGroup := C.CString(outputGroup)
	defer C.free(unsafe.Pointer(cGroup))

	var cIndex *C.char
	if defaultIndex != "" {
		cIndex = C.CString(defaultIndex)
		defer C.free(unsafe.Pointer(cIndex))
	}

	handle := C.splunk_pipeline_create_output_group(cGroup, cIndex)
	if handle == nil {
		return nil, fmt.Errorf("splunk_pipeline_create_output_group failed: %s",
			C.GoString(C.splunk_pipeline_last_error()))
	}
	return &cPipeline{handle: handle}, nil
}

// ── cPipeline ─────────────────────────────────────────────────────────────────

type cPipeline struct {
	handle  *C.SplunkPipeline
	id      int32
	ch      chan splunkapi.Event // nil for output-only
	stopped atomic.Bool
	mu      sync.Mutex
}

func (p *cPipeline) Start() error {
	if rc := C.splunk_pipeline_start(p.handle); rc != 0 {
		return fmt.Errorf("splunk_pipeline_start failed: %s",
			C.GoString(C.splunk_pipeline_last_error()))
	}
	return nil
}

func (p *cPipeline) Events() <-chan splunkapi.Event {
	if p.ch == nil {
		return nil
	}
	return p.ch
}

func (p *cPipeline) Send(body []byte, source, sourcetype, host, index string) error {
	return p.SendToGroup(body, source, sourcetype, host, index, "")
}

func (p *cPipeline) SendToGroup(body []byte, source, sourcetype, host, index, outputGroup string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.handle == nil {
		return fmt.Errorf("splunk_pipeline: already stopped")
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

	var cGroup *C.char
	if outputGroup != "" {
		cGroup = C.CString(outputGroup)
		defer C.free(unsafe.Pointer(cGroup))
	}

	rc := C.splunk_pipeline_send_to_group(p.handle,
		cGroup,
		cData, C.size_t(len(body)),
		cSource, cSourcetype, cHost, cIndex)
	if rc != 0 {
		return fmt.Errorf("splunk_pipeline_send_to_group: %s",
			C.GoString(C.splunk_pipeline_last_error()))
	}
	return nil
}

func (p *cPipeline) Stop(drainSeconds int) {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}
	// Remove from registry first so no new events are dispatched.
	if p.ch != nil {
		pipelineRegistry.Delete(p.id)
	}
	p.mu.Lock()
	C.splunk_pipeline_stop(p.handle, C.int(drainSeconds))
	p.mu.Unlock()
	// Close the channel after stop so consumeLoop exits cleanly.
	if p.ch != nil {
		close(p.ch)
	}
}

func (p *cPipeline) Destroy() {
	C.splunk_pipeline_destroy(p.handle)
	p.handle = nil
}
