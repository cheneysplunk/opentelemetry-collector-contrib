// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

// splunktailReceiver implements receiver.Logs using the Splunk tail pipeline
// exposed by splunkframeworkextension via the splunkapi.SplunkFramework
// interface.  No CGo in this file — all C library calls live in the extension.
type splunktailReceiver struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs

	tailInput splunkapi.TailInput

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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

// Start locates splunkframeworkextension from the collector host, creates a
// TailInput session, registers all configured monitors, and launches the
// consumer goroutine.
func (r *splunktailReceiver) Start(_ context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tailInput != nil {
		return nil // already started
	}

	// Locate the extension that implements SplunkFramework.
	var fw splunkapi.SplunkFramework
	for _, ext := range host.GetExtensions() {
		if f, ok := ext.(splunkapi.SplunkFramework); ok {
			fw = f
			break
		}
	}
	if fw == nil {
		return fmt.Errorf("splunktailreceiver: splunkframeworkextension not found; add it to service.extensions")
	}

	ti, err := fw.NewTailInput(splunkapi.TailConfig{
		DefaultSourcetype: r.cfg.DefaultSourcetype,
		DefaultIndex:      r.cfg.DefaultIndex,
		Host:              r.cfg.Host,
		FishbucketDir:     r.cfg.FishbucketDir,
	})
	if err != nil {
		return fmt.Errorf("splunktailreceiver: %w", err)
	}

	for _, m := range r.cfg.Monitors {
		if err := ti.AddMonitor(splunkapi.MonitorConfig{
			Glob:       m.Glob,
			Sourcetype: m.Sourcetype,
			Index:      m.Index,
			Host:       m.Host,
		}); err != nil {
			ti.Destroy()
			return fmt.Errorf("splunktailreceiver: %w", err)
		}
	}

	if err := ti.Start(); err != nil {
		ti.Destroy()
		return fmt.Errorf("splunktailreceiver: %w", err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.tailInput = ti

	r.wg.Add(1)
	go r.consumeLoop(r.ctx, ti.Events())

	r.logger.Info("splunktail receiver started",
		zap.Int("monitors", len(r.cfg.Monitors)),
	)
	return nil
}

// consumeLoop reads TailEvents from the channel and forwards them to the
// pipeline consumer.  Exits when the channel is closed (by Stop).
func (r *splunktailReceiver) consumeLoop(ctx context.Context, events <-chan splunkapi.TailEvent) {
	defer r.wg.Done()
	for ev := range events {
		r.deliver(ctx, ev)
	}
}

// deliver converts one TailEvent to a plog.Logs and calls ConsumeLogs.
func (r *splunktailReceiver) deliver(ctx context.Context, ev splunkapi.TailEvent) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", ev.Host)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("splunktail")
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ev.Time))
	lr.Body().SetStr(ev.Body)
	lr.Attributes().PutStr("splunk.source", ev.Source)
	lr.Attributes().PutStr("splunk.sourcetype", ev.Sourcetype)

	if err := r.nextConsumer.ConsumeLogs(ctx, ld); err != nil {
		r.logger.Warn("ConsumeLogs error", zap.Error(err))
	}
}

// Shutdown stops the tail pipeline and waits for the consumer goroutine.
func (r *splunktailReceiver) Shutdown(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tailInput == nil {
		return nil
	}

	r.cancel()

	// Stop closes the Events() channel, which causes consumeLoop to exit.
	r.tailInput.Stop()
	r.wg.Wait()

	r.tailInput.Destroy()
	r.tailInput = nil

	r.logger.Info("splunktail receiver shut down")
	return nil
}


