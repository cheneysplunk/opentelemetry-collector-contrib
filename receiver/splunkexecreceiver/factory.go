// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkexecreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkexecreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkexecreceiver/internal/metadata"
)

// NewFactory returns a receiver.Factory for native Splunk scripted inputs.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Framework: component.NewID(component.MustNewType("splunkframework")),
	}
}

func createLogsReceiver(
	ctx context.Context,
	params receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	return newLogsReceiver(ctx, params, cfg.(*Config), nextConsumer)
}
