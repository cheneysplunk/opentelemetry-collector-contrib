// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// MonitorConfig is a legacy receiver-side monitor declaration.
// It is accepted for config compatibility but ignored. Create monitor://
// stanzas in the extension-owned inputs.conf instead.
type MonitorConfig struct {
	// Glob is a shell-style glob pattern or exact file path. Required.
	Glob string `mapstructure:"glob"`

	// Sourcetype overrides the default sourcetype for events from this monitor.
	// Empty string → use Config.DefaultSourcetype.
	Sourcetype string `mapstructure:"sourcetype"`

	// Index is the Splunk index name for events from this monitor.
	// Empty string → use Config.DefaultIndex.
	Index string `mapstructure:"index"`

	// Host overrides the host field for events from this monitor.
	// Empty string → use Config.Host (or local hostname).
	Host string `mapstructure:"host"`
}

// Config defines configuration for the Splunk tail-input receiver.
type Config struct {
	// Framework is the component ID of the splunkframeworkextension to use.
	// Defaults to "splunkframework". Must be listed in service.extensions.
	Framework component.ID `mapstructure:"framework"`

	// Monitors is deprecated and ignored. The receiver now reads from the
	// splunkframeworkextension-owned inputs.conf cache.
	Monitors []MonitorConfig `mapstructure:"monitors"`

	// DefaultSourcetype is deprecated and ignored. Configure sourcetype in
	// inputs.conf monitor:// stanzas.
	DefaultSourcetype string `mapstructure:"default_sourcetype"`

	// DefaultIndex is deprecated and ignored. Configure index in inputs.conf.
	DefaultIndex string `mapstructure:"default_index"`

	// Host is deprecated and ignored. Configure host in inputs.conf or through
	// the native Splunk defaults.
	Host string `mapstructure:"host"`

	// FishbucketDir is deprecated and ignored. The extension-owned Splunk
	// framework uses the native fishbucket location from SPLUNK_DB.
	FishbucketDir string `mapstructure:"fishbucket_dir"`

	// SplunkHome is deprecated and ignored. Set splunk_home on the
	// splunkframeworkextension instead.
	SplunkHome string `mapstructure:"splunk_home"`
}

// Validate checks that the configuration is well-formed.
func (c *Config) Validate() error {
	if c.Framework == (component.ID{}) {
		return fmt.Errorf("splunktailreceiver: framework must not be empty; set it to the extension id (e.g. splunkframework)")
	}
	return nil
}
