// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package splunkframeworkextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

// splunkFrameworkExtension is a no-op stub on non-Linux platforms.
type splunkFrameworkExtension struct{}

func newSplunkFrameworkExtension(_ extension.Settings, _ *Config) *splunkFrameworkExtension {
	return &splunkFrameworkExtension{}
}

func (e *splunkFrameworkExtension) Start(_ context.Context, _ component.Host) error {
	return fmt.Errorf("splunkframeworkextension: only supported on Linux")
}

func (e *splunkFrameworkExtension) Shutdown(_ context.Context) error {
	return nil
}
