// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter/internal/metadata"
)

// NewFactory returns an exporter.Factory for the Splunk S2S TCP output exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Framework:         component.NewID(component.MustNewType("splunkframework")),
		DefaultSourcetype: DefaultSourcetype,
		DrainSeconds:      DefaultDrainSeconds,
	}
}

func createLogsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	return newLogsExporter(ctx, params, cfg.(*Config))
}
