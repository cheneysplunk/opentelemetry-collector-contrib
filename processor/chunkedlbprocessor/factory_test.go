// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFactory_TypeAndStability(t *testing.T) {
	f := NewFactory()
	require.Equal(t, "chunkedlb", f.Type().String())
}

func TestFactory_DefaultConfigValidates(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
	// Default = "never": no OTel-contrib exporter understands the doneKey
	// marker today, so we ship boundary-marker-off by default.  Operators
	// flip to "always" once they have a marker-aware downstream.  See
	// design doc §6.0.4 and factory.go createDefaultConfig comment.
	require.Equal(t, DoneKeyEmissionNever, cfg.DoneKeyEmission)
	require.True(t, cfg.DefaultRule.EventBreakerEnable)
}
