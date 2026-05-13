// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/metadata"
)

const (
	defaultSourcetypeAttribute = "com.splunk.sourcetype"
	defaultDoneKeyAttribute    = "com.splunk.done_key"
	defaultEventBreaker        = `([\r\n]+)`
	defaultLBChunkBreaker      = `([\r\n]+)`
	defaultLBChunkTruncate     = 2 * 1000 * 1000 // matches Splunk default of 2,000,000

	defaultMatchLimit      uint32 = 1000000
	defaultDepthLimit      uint32 = 1000
	defaultTimeoutMS              = 50
	defaultMaxPatternBytes        = 4096
	defaultMaxCachedRules         = 10000
)

// NewFactory returns the OTel processor factory for the chunkedlb processor.
// Logs-only by design (mirrors the Splunk C++ ChunkedLBProcessor's role in the
// UF/HF logs pipeline).  See plan §1.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		SourcetypeAttribute: defaultSourcetypeAttribute,
		DoneKeyAttribute:    defaultDoneKeyAttribute,
		// Default = "never": no OTel-contrib exporter (including
		// splunk_hec) currently understands the doneKey marker, so
		// emitting it by default produces phantom empty events
		// (`splunk_hec` → /event) or silently dropped boundary signals
		// (`splunk_hec` → /raw, otlp, kafka, …).  Operators must opt
		// in via `done_key_emission: always` once they have a
		// marker-aware downstream (a future Splunk-aware exporter or a
		// Tier-2 SplunkLineBreakingProcessor).  See design doc §6.0.4
		// for the full exporter compatibility matrix.
		DoneKeyEmission: DoneKeyEmissionNever,
		DefaultRule: RuleConfig{
			EventBreakerEnable: true,
			EventBreaker:       defaultEventBreaker,
		},
		Rules:          map[string]RuleConfig{},
		CPUProfiling:   false,
		HTTPOutCompat:  false,
		MaxCachedRules: defaultMaxCachedRules,
		Regex: RegexLimits{
			MatchLimit:      defaultMatchLimit,
			DepthLimit:      defaultDepthLimit,
			TimeoutMS:       defaultTimeoutMS,
			MaxPatternBytes: defaultMaxPatternBytes,
		},
	}
}

func createLogsProcessor(
	ctx context.Context,
	settings processor.Settings,
	cfg component.Config,
	next consumer.Logs,
) (processor.Logs, error) {
	pCfg := cfg.(*Config)
	p, err := newChunkedLBProcessor(settings, pCfg)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(
		ctx,
		settings,
		cfg,
		next,
		p.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
}
