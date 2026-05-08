// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

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

// splunktailReceiver implements receiver.Logs via the generic
// splunkapi.Pipeline interface — no CGo in this file.
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

// buildInputsConf converts structured receiver config into a raw inputs.conf
// stanza string understood by splunk_pipeline_create().
func (r *splunktailReceiver) buildInputsConf() string {
	var sb strings.Builder
	// [default] carries top-level TailinConfig fields
	sb.WriteString("[default]\n")
	if r.cfg.DefaultSourcetype != "" {
		fmt.Fprintf(&sb, "default_sourcetype = %s\n", r.cfg.DefaultSourcetype)
	}
	if r.cfg.DefaultIndex != "" {
		fmt.Fprintf(&sb, "default_index = %s\n", r.cfg.DefaultIndex)
	}
	if r.cfg.Host != "" {
		fmt.Fprintf(&sb, "host = %s\n", r.cfg.Host)
	}
	if r.cfg.FishbucketDir != "" {
		fmt.Fprintf(&sb, "fishbucket_dir = %s\n", r.cfg.FishbucketDir)
	}
	// One [monitor://...] stanza per configured glob
	for _, m := range r.cfg.Monitors {
		fmt.Fprintf(&sb, "[monitor://%s]\n", m.Glob)
		if m.Sourcetype != "" {
			fmt.Fprintf(&sb, "sourcetype = %s\n", m.Sourcetype)
		}
		if m.Index != "" {
			fmt.Fprintf(&sb, "index = %s\n", m.Index)
		}
		if m.Host != "" {
			fmt.Fprintf(&sb, "host = %s\n", m.Host)
		}
	}
	return sb.String()
}

// Start retrieves splunkframeworkextension, creates an input Pipeline from
// the receiver's config stanzas, and launches the consumer goroutine.
func (r *splunktailReceiver) Start(_ context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pipeline != nil {
		return nil
	}

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

	p, err := fw.NewPipeline(splunkapi.PipelineConfig{
		InputsConf: r.buildInputsConf(),
	})
	if err != nil {
		return fmt.Errorf("splunktailreceiver: %w", err)
	}

	if err := p.Start(); err != nil {
		p.Destroy()
		return fmt.Errorf("splunktailreceiver: %w", err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.pipeline = p

	r.wg.Add(1)
	go r.consumeLoop(r.ctx, p.Events())

	r.logger.Info("splunktail receiver started",
		zap.Int("monitors", len(r.cfg.Monitors)),
	)
	return nil
}

// consumeLoop reads Events from the pipeline channel and forwards them to
// the OTel pipeline. Exits when the channel is closed by Stop.
func (r *splunktailReceiver) consumeLoop(ctx context.Context, events <-chan splunkapi.Event) {
	defer r.wg.Done()
	for ev := range events {
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
}

// Shutdown stops the pipeline and waits for the consumer goroutine to exit.
func (r *splunktailReceiver) Shutdown(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pipeline == nil {
		return nil
	}

	r.cancel()
	r.pipeline.Stop(5) // Stop closes the Events channel → consumeLoop exits
	r.wg.Wait()
	r.pipeline.Destroy()
	r.pipeline = nil

	r.logger.Info("splunktail receiver shut down")
	return nil
}


