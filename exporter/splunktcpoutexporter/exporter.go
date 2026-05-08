// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

// splunktcpoutExporter implements exporter.Logs using the Splunk S2S TCP
// output pipeline exposed by splunkframeworkextension via
// splunkapi.SplunkFramework.  No CGo in this file.
type splunktcpoutExporter struct {
	cfg    *Config
	logger *zap.Logger
	out    splunkapi.TcpOutput
	mu     sync.Mutex
}

// newLogsExporter constructs an exporter.Logs backed by the Splunk S2S library.
// The TcpOutput session is created lazily in start().
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

// start locates splunkframeworkextension and opens the S2S connection.
func (e *splunktcpoutExporter) start(_ context.Context, host component.Host) error {
	var fw splunkapi.SplunkFramework
	for _, ext := range host.GetExtensions() {
		if f, ok := ext.(splunkapi.SplunkFramework); ok {
			fw = f
			break
		}
	}
	if fw == nil {
		return fmt.Errorf("splunktcpoutexporter: splunkframeworkextension not found; add it to service.extensions")
	}

	out, err := fw.NewTcpOutput(e.cfg.Host, e.cfg.Port, e.cfg.Index)
	if err != nil {
		return fmt.Errorf("splunktcpoutexporter: %w", err)
	}

	e.out = out
	e.logger.Info("splunktcpout exporter started",
		zap.String("host", e.cfg.Host),
		zap.Int("port", e.cfg.Port),
		zap.String("index", e.cfg.Index),
	)
	return nil
}

// shutdown drains the send queue and closes the S2S connection.
func (e *splunktcpoutExporter) shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.out != nil {
		e.out.Destroy(e.cfg.DrainSeconds)
		e.out = nil
		e.logger.Info("splunktcpout exporter shut down")
	}
	return nil
}

// pushLogsData iterates all log records and forwards each as an S2S event.
func (e *splunktcpoutExporter) pushLogsData(_ context.Context, ld plog.Logs) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.out == nil {
		return errors.New("splunktcpout: exporter not started")
	}

	var errs []error
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
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

// sendLogRecord resolves Splunk metadata fields and sends one event.
//
// Attribute resolution order:
//
//	source      → log attr "splunk.source"      → Config.DefaultSource
//	sourcetype  → log attr "splunk.sourcetype"   → Config.DefaultSourcetype
//	host        → log attr "host.name"           → resource "host.name" → Config.DefaultHost
//	index       → log attr "splunk.index"        → Config.Index (may be empty)
func (e *splunktcpoutExporter) sendLogRecord(lr plog.LogRecord, defaultHost string) error {
	raw := logBody(lr)
	source := logAttr(lr, "splunk.source", e.cfg.DefaultSource)
	sourcetype := logAttr(lr, "splunk.sourcetype", e.cfg.DefaultSourcetype)
	hostField := logAttr(lr, "host.name", defaultHost)
	index := logAttr(lr, "splunk.index", e.cfg.Index)

	return e.out.Send([]byte(raw), source, sourcetype, hostField, index)
}

// ── helpers ──────────────────────────────────────────────────────────────────

func logBody(lr plog.LogRecord) string {
	b := lr.Body()
	if b.Type() == pcommon.ValueTypeStr {
		return b.Str()
	}
	return b.AsString()
}

func logAttr(lr plog.LogRecord, key, fallback string) string {
	if v, ok := lr.Attributes().Get(key); ok {
		return v.AsString()
	}
	return fallback
}

func resourceAttr(r pcommon.Resource, key, fallback string) string {
	if v, ok := r.Attributes().Get(key); ok {
		return v.AsString()
	}
	return fallback
}

