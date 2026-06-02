// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package splunkexecreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkexecreceiver"

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
	return nil, errors.New("splunkexecreceiver is only supported on Linux")
}

type splunkexecReceiver struct{}

func (r *splunkexecReceiver) Start(_ context.Context, _ component.Host) error {
	return errors.New("splunkexecreceiver is only supported on Linux")
}

func (r *splunkexecReceiver) Shutdown(_ context.Context) error { return nil }
