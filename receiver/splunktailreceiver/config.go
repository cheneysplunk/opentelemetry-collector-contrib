// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"

import (
	"errors"
	"fmt"
)

// MonitorConfig describes a single file-glob to watch.
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
	// Monitors is the list of file globs to watch. At least one is required.
	Monitors []MonitorConfig `mapstructure:"monitors"`

	// DefaultSourcetype is the Splunk sourcetype used for monitors that do not
	// specify their own sourcetype. Default: "tailin".
	DefaultSourcetype string `mapstructure:"default_sourcetype"`

	// DefaultIndex is the Splunk index used for monitors that do not specify
	// their own index. Default: "main".
	DefaultIndex string `mapstructure:"default_index"`

	// Host is the host field written to every log record. Defaults to the
	// system hostname (gethostname).
	Host string `mapstructure:"host"`

	// FishbucketDir is the path to a writable directory for fish-bucket state
	// files (file-position persistence). Empty → $SPLUNK_DB/fishbucket.
	FishbucketDir string `mapstructure:"fishbucket_dir"`

	// SplunkHome, when non-empty, is written to the SPLUNK_HOME environment
	// variable before the C library is initialised. It must point to a Splunk
	// installation directory containing an etc/ sub-tree.
	SplunkHome string `mapstructure:"splunk_home"`
}

// Validate checks that the configuration is well-formed.
func (c *Config) Validate() error {
	if len(c.Monitors) == 0 {
		return errors.New("splunktailreceiver: at least one monitor must be configured")
	}
	for i, m := range c.Monitors {
		if m.Glob == "" {
			return fmt.Errorf("splunktailreceiver: monitors[%d].glob must not be empty", i)
		}
	}
	return nil
}
