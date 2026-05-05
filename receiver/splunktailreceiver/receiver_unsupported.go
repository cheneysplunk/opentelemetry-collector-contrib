// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

func newLogsReceiver(
	_ context.Context,
	_ receiver.Settings,
	_ *Config,
	_ consumer.Logs,
) (receiver.Logs, error) {
	return nil, errors.New("splunktailreceiver is only supported on Linux")
}

type splunktailReceiver struct{}

func (r *splunktailReceiver) Start(_ context.Context, _ component.Host) error {
	return errors.New("splunktailreceiver is only supported on Linux")
}

func (r *splunktailReceiver) Shutdown(_ context.Context) error { return nil }
