// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"

import "errors"

const (
	// DefaultPort is the standard Splunk S2S receive port.
	DefaultPort = 9997
	// DefaultSourcetype is the Splunk sourcetype assigned when a log record
	// carries no "splunk.sourcetype" attribute.
	DefaultSourcetype = "otel"
	// DefaultDrainSeconds is how long tcpout_destroy() waits for the internal
	// send queue to drain before forcing shutdown.
	DefaultDrainSeconds = 5
)

// Config defines configuration for the Splunk S2S TCP output exporter.
type Config struct {
	// Host is the Splunk indexer hostname or IP address. Required.
	Host string `mapstructure:"host"`

	// Port is the Splunk indexer S2S receive port (default: 9997).
	Port int `mapstructure:"port"`

	// Index is the default Splunk index for forwarded events.
	// Individual log records may override this via the "splunk.index"
	// attribute.
	Index string `mapstructure:"index"`

	// DefaultSource is the Splunk source field used when the log record
	// has no "splunk.source" attribute.
	DefaultSource string `mapstructure:"default_source"`

	// DefaultSourcetype is the Splunk sourcetype used when the log record
	// has no "splunk.sourcetype" attribute (default: "otel").
	DefaultSourcetype string `mapstructure:"default_sourcetype"`

	// DefaultHost is the Splunk host field used when neither the log record
	// nor its resource has a "host.name" attribute.
	DefaultHost string `mapstructure:"default_host"`

	// DrainSeconds is how long tcpout_destroy() waits for the send queue to
	// drain before forcing shutdown (default: 5).
	DrainSeconds int `mapstructure:"drain_seconds"`

	// SplunkHome, when non-empty, is written to the SPLUNK_HOME environment
	// variable before the C library is initialised.  It must point to a
	// directory containing an etc/ sub-tree (e.g. /opt/splunk or a minimal
	// UF skeleton).  If empty the existing SPLUNK_HOME environment variable
	// is used.
	SplunkHome string `mapstructure:"splunk_home"`
}

// Validate checks the configuration for required fields and sane values.
func (c *Config) Validate() error {
	if c.Host == "" {
		return errors.New("host is required")
	}
	if c.Port < 1 || c.Port > 65535 {
		return errors.New("port must be in the range 1–65535")
	}
	if c.DrainSeconds < 0 {
		return errors.New("drain_seconds must be non-negative")
	}
	return nil
}
