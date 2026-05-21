// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkframeworkextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"

// Config holds the configuration for splunkframeworkextension.
type Config struct {
	// SplunkHome is the path to the Splunk installation directory.
	// Sets $SPLUNK_HOME for the C++ framework.
	// Required.
	SplunkHome string `mapstructure:"splunk_home"`

	// SplunkDB is the path to the Splunk database / state directory.
	// Sets $SPLUNK_DB for the C++ framework (fishbucket lives here).
	// Defaults to $SPLUNK_HOME/var/lib/splunk when empty.
	SplunkDB string `mapstructure:"splunk_db"`

	// ManagementPort starts the native Splunk REST management server when set.
	// The supported REST surface is auth/login plus services/configs/conf-*.
	// Leave as zero to disable the listener while keeping the in-process
	// ConfManager cache/API enabled.
	ManagementPort int `mapstructure:"management_port"`
}
