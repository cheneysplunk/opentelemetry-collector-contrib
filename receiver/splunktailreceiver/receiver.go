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

type splunktailReceiver struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs
	pipeline     splunkapi.Pipeline
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
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

// Start locates splunkframeworkextension, asks it to create a native
// TailManager pipeline from the merged inputs.conf cache, and forwards all
// monitor input events delivered by that pipeline into the OTel logs pipeline.
func (r *splunktailReceiver) Start(_ context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pipeline != nil {
		return nil
	}

	ext, ok := host.GetExtensions()[r.cfg.Framework]
	if !ok {
		return fmt.Errorf("splunktailreceiver: extension %q not found; add it to service.extensions", r.cfg.Framework)
	}
	fw, ok := ext.(splunkapi.SplunkFramework)
	if !ok {
		return fmt.Errorf("splunktailreceiver: extension %q does not implement splunkapi.SplunkFramework", r.cfg.Framework)
	}

	if len(r.cfg.Monitors) > 0 {
		r.logger.Warn("splunktailreceiver monitors config is ignored; create monitor:// stanzas in inputs.conf instead",
			zap.Int("ignored_monitors", len(r.cfg.Monitors)),
		)
	}
	if r.cfg.SplunkHome != "" || r.cfg.DefaultSourcetype != "" ||
		r.cfg.DefaultIndex != "" || r.cfg.Host != "" || r.cfg.FishbucketDir != "" {
		r.logger.Warn("legacy splunktailreceiver input defaults are ignored; splunkframeworkextension owns SPLUNK_HOME and native inputs.conf")
	}

	p, err := fw.NewMonitorPipeline()
	if err != nil {
		return fmt.Errorf("splunktailreceiver: %w", err)
	}
	events := p.Events()
	if events == nil {
		p.Destroy()
		return fmt.Errorf("splunktailreceiver: monitor pipeline did not expose an event channel")
	}
	if err := p.Start(); err != nil {
		p.Destroy()
		return fmt.Errorf("splunktailreceiver: %w", err)
	}

	r.pipeline = p
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go r.consumeLoop(r.ctx, events)

	r.logger.Info("splunktail receiver started",
		zap.String("framework", r.cfg.Framework.String()),
		zap.String("config_source", "inputs.conf"),
	)
	return nil
}

func (r *splunktailReceiver) consumeLoop(ctx context.Context, events <-chan splunkapi.Event) {
	defer r.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			r.forward(ctx, ev)
		}
	}
}

func (r *splunktailReceiver) forward(ctx context.Context, ev splunkapi.Event) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	if ev.Host != "" {
		rl.Resource().Attributes().PutStr("host.name", ev.Host)
	}
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("splunktail")
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ev.Time))
	if len(ev.RawBody) > 0 {
		lr.Body().SetEmptyBytes().FromRaw(ev.RawBody)
	} else {
		lr.Body().SetStr(ev.Body)
	}
	if ev.Source != "" {
		lr.Attributes().PutStr("splunk.source", ev.Source)
	}
	if ev.Sourcetype != "" {
		lr.Attributes().PutStr("splunk.sourcetype", ev.Sourcetype)
	}
	if err := r.nextConsumer.ConsumeLogs(ctx, ld); err != nil {
		r.logger.Warn("ConsumeLogs error", zap.Error(err))
	}
}

// Shutdown stops the extension-owned pipeline session and waits for the event
// forwarding goroutine to exit. The framework lifecycle remains owned by
// splunkframeworkextension.
func (r *splunktailReceiver) Shutdown(_ context.Context) error {
	r.mu.Lock()
	p := r.pipeline
	cancel := r.cancel
	r.pipeline = nil
	r.cancel = nil
	r.mu.Unlock()

	if p == nil {
		return nil
	}

	p.Stop(0)
	r.wg.Wait()
	if cancel != nil {
		cancel()
	}
	p.Destroy()

	r.logger.Info("splunktail receiver shut down")
	return nil
}
