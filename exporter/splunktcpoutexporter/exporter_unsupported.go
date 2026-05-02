// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/exporter"
)

func newLogsExporter(_ context.Context, _ exporter.Settings, _ *Config) (exporter.Logs, error) {
	return nil, errors.New("splunktcpout exporter is only supported on Linux")
}
