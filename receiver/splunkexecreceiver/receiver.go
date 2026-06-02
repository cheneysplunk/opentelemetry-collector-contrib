// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkexecreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkexecreceiver"

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

type splunkexecReceiver struct {
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
	return &splunkexecReceiver{
		cfg:          cfg,
		logger:       params.Logger,
		nextConsumer: nextConsumer,
	}, nil
}

func (r *splunkexecReceiver) Start(_ context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pipeline != nil {
		return nil
	}

	ext, ok := host.GetExtensions()[r.cfg.Framework]
	if !ok {
		return fmt.Errorf("splunkexecreceiver: extension %q not found; add it to service.extensions", r.cfg.Framework)
	}
	fw, ok := ext.(splunkapi.SplunkFramework)
	if !ok {
		return fmt.Errorf("splunkexecreceiver: extension %q does not implement splunkapi.SplunkFramework", r.cfg.Framework)
	}

	inputsConf := r.renderInputsConf()
	p, err := fw.NewPipeline(splunkapi.PipelineConfig{
		InputsConf: inputsConf,
		PropsConf:  r.cfg.PropsConf,
	})
	if err != nil {
		return fmt.Errorf("splunkexecreceiver: %w", err)
	}

	events := p.Events()
	if events == nil {
		p.Destroy()
		return fmt.Errorf("splunkexecreceiver: exec pipeline did not expose an event channel")
	}
	if err := p.Start(); err != nil {
		p.Destroy()
		return fmt.Errorf("splunkexecreceiver: %w", err)
	}

	r.pipeline = p
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go r.consumeLoop(r.ctx, events)

	r.logger.Info("splunkexec receiver started",
		zap.String("framework", r.cfg.Framework.String()),
		zap.Int("script_count", len(r.cfg.Scripts)),
		zap.Bool("inputs_conf", strings.TrimSpace(r.cfg.InputsConf) != ""),
	)
	return nil
}

func (r *splunkexecReceiver) renderInputsConf() string {
	var b strings.Builder
	if raw := strings.TrimSpace(r.cfg.InputsConf); raw != "" {
		b.WriteString(raw)
		b.WriteString("\n\n")
	}
	for _, script := range r.cfg.Scripts {
		command := script.Command
		if !strings.HasPrefix(command, "script://") {
			command = "script://" + command
		}
		b.WriteString("[")
		b.WriteString(command)
		b.WriteString("]\n")
		b.WriteString("disabled = 0\n")
		interval := script.Interval
		if strings.TrimSpace(interval) == "" {
			interval = "60"
		}
		writeConfKV(&b, "interval", interval)
		writeConfKV(&b, "sourcetype", script.Sourcetype)
		writeConfKV(&b, "index", script.Index)
		writeConfKV(&b, "host", script.Host)
		writeConfKV(&b, "source", script.Source)
		writeConfKV(&b, "start_by_shell", fmt.Sprintf("%t", script.StartByShell))
		b.WriteString("\n")
	}
	return b.String()
}

func writeConfKV(b *strings.Builder, key, value string) {
	if value == "" {
		return
	}
	b.WriteString(key)
	b.WriteString(" = ")
	b.WriteString(value)
	b.WriteString("\n")
}

func (r *splunkexecReceiver) consumeLoop(ctx context.Context, events <-chan splunkapi.Event) {
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

func (r *splunkexecReceiver) forward(ctx context.Context, ev splunkapi.Event) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	if ev.Host != "" {
		rl.Resource().Attributes().PutStr("host.name", ev.Host)
	}
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("splunkexec")
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

func (r *splunkexecReceiver) Shutdown(_ context.Context) error {
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

	r.logger.Info("splunkexec receiver shut down")
	return nil
}
