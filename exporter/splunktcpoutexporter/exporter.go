// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"

/*
// tcpout_cabi.h is vendored from main/src/output/tcpout_lib/tcpout_cabi.h.
// Provide its include path at build time via:
//   CGO_CFLAGS="-I/path/to/tcpout_lib"
// and the library path via:
//   CGO_LDFLAGS="-L/path/to/tcpout_lib -L/path/to/splunk_home/lib \
//                -Wl,-rpath,/path/to/tcpout_lib \
//                -Wl,-rpath,/path/to/splunk_home/lib"

#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -ltcpout_cabi -lstdc++ -ldl -lpthread
#include "tcpout_cabi.h"
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// splunktcpoutExporter wraps a single libtcpout_cabi.so session handle.
// One handle = one connection to one Splunk indexer.
type splunktcpoutExporter struct {
	cfg    *Config
	logger *zap.Logger
	handle *C.TcpoutHandle
	mu     sync.Mutex // serialises concurrent pushLogsData calls
}

// newLogsExporter constructs an exporter.Logs backed by libtcpout_cabi.so.
// The C handle is created lazily in start().
func newLogsExporter(ctx context.Context, params exporter.Settings, cfg *Config) (exporter.Logs, error) {
	exp := &splunktcpoutExporter{
		cfg:    cfg,
		logger: params.Logger,
	}
	return exporterhelper.NewLogs(
		ctx,
		params,
		cfg,
		exp.pushLogsData,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

// start initialises the Splunk framework and opens the S2S connection to the
// configured indexer.  Called once by the collector lifecycle.
func (e *splunktcpoutExporter) start(_ context.Context, _ component.Host) error {
	if e.cfg.SplunkHome != "" {
		if err := os.Setenv("SPLUNK_HOME", e.cfg.SplunkHome); err != nil {
			return fmt.Errorf("failed to set SPLUNK_HOME: %w", err)
		}
	}

	cHost := C.CString(e.cfg.Host)
	defer C.free(unsafe.Pointer(cHost))

	// Pass NULL for index when not configured — the indexer will use its default.
	var cIndex *C.char
	if e.cfg.Index != "" {
		cIndex = C.CString(e.cfg.Index)
		defer C.free(unsafe.Pointer(cIndex))
	}

	// tcpout_create bootstraps the Splunk framework and performs the S2S
	// handshake.  Lock the OS thread for the duration as the C init code
	// uses thread-local state (mirrors go_tcpout.go behaviour).
	runtime.LockOSThread()
	handle := C.tcpout_create(cHost, C.int(e.cfg.Port), cIndex)
	runtime.UnlockOSThread()

	if handle == nil {
		return fmt.Errorf("tcpout_create(%s:%d) failed: %s",
			e.cfg.Host, e.cfg.Port, C.GoString(C.tcpout_last_error()))
	}

	e.handle = handle
	e.logger.Info("splunktcpout exporter started",
		zap.String("host", e.cfg.Host),
		zap.Int("port", e.cfg.Port),
		zap.String("index", e.cfg.Index),
	)
	return nil
}

// shutdown flushes queued events and tears down the S2S connection.
func (e *splunktcpoutExporter) shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.handle != nil {
		C.tcpout_destroy(e.handle, C.int(e.cfg.DrainSeconds))
		e.handle = nil
		e.logger.Info("splunktcpout exporter shut down")
	}
	return nil
}

// pushLogsData iterates over all log records in ld and sends each one as an
// S2S event.  Errors from individual records are collected and returned as a
// joined error so the exporter helper can retry the whole batch if desired.
func (e *splunktcpoutExporter) pushLogsData(_ context.Context, ld plog.Logs) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.handle == nil {
		return errors.New("splunktcpout: exporter not started")
	}

	var errs []error
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		// "host.name" on the resource is the S2S host field default for all
		// records within this resource scope.
		defaultHost := resourceAttr(rl.Resource(), "host.name", e.cfg.DefaultHost)

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				if err := e.sendLogRecord(sl.LogRecords().At(k), defaultHost); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

// sendLogRecord converts one OTel log record to an S2S cooked-stream event.
//
// Attribute resolution order for each Splunk metadata field:
//
//	source      → log attr "splunk.source"     → Config.DefaultSource
//	sourcetype  → log attr "splunk.sourcetype"  → Config.DefaultSourcetype
//	host        → log attr "host.name"          → resource "host.name" → Config.DefaultHost
//	index       → log attr "splunk.index"       → Config.Index (may be empty)
func (e *splunktcpoutExporter) sendLogRecord(lr plog.LogRecord, defaultHost string) error {
	raw := logBody(lr)
	source := logAttr(lr, "splunk.source", e.cfg.DefaultSource)
	sourcetype := logAttr(lr, "splunk.sourcetype", e.cfg.DefaultSourcetype)
	hostField := logAttr(lr, "host.name", defaultHost)
	index := logAttr(lr, "splunk.index", e.cfg.Index)

	cData := C.CString(raw)
	defer C.free(unsafe.Pointer(cData))
	cSource := C.CString(source)
	defer C.free(unsafe.Pointer(cSource))
	cSourcetype := C.CString(sourcetype)
	defer C.free(unsafe.Pointer(cSourcetype))
	cHostField := C.CString(hostField)
	defer C.free(unsafe.Pointer(cHostField))

	// Pass NULL for index when unset — the indexer uses its default.
	var cIndex *C.char
	if index != "" {
		cIndex = C.CString(index)
		defer C.free(unsafe.Pointer(cIndex))
	}

	rc := C.tcpout_send(
		e.handle,
		cData, C.size_t(len(raw)),
		cSource, cSourcetype, cHostField, cIndex,
	)
	if rc != 0 {
		return fmt.Errorf("tcpout_send: %s", C.GoString(C.tcpout_last_error()))
	}
	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// logBody returns the string value of a log record body.  Structured bodies
// are serialised with AsString().
func logBody(lr plog.LogRecord) string {
	b := lr.Body()
	if b.Type() == pcommon.ValueTypeStr {
		return b.Str()
	}
	return b.AsString()
}

// logAttr returns the string value of a log-record attribute, or fallback.
func logAttr(lr plog.LogRecord, key, fallback string) string {
	if v, ok := lr.Attributes().Get(key); ok {
		return v.AsString()
	}
	return fallback
}

// resourceAttr returns the string value of a resource attribute, or fallback.
func resourceAttr(r pcommon.Resource, key, fallback string) string {
	if v, ok := r.Attributes().Get(key); ok {
		return v.AsString()
	}
	return fallback
}
