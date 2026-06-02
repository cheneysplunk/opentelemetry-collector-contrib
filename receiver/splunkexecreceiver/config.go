// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkexecreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkexecreceiver"

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/component"
)

// ScriptConfig describes one native Splunk scripted input stanza.
type ScriptConfig struct {
	// Command is the script command/path for [script://<command>].
	Command string `mapstructure:"command"`

	// Interval is the native Splunk interval value. Empty defaults to "60".
	// Use native values such as "60", "-1", or a cron expression.
	Interval string `mapstructure:"interval"`

	Sourcetype string `mapstructure:"sourcetype"`
	Index      string `mapstructure:"index"`
	Host       string `mapstructure:"host"`
	Source     string `mapstructure:"source"`

	// StartByShell maps to inputs.conf start_by_shell. The receiver defaults
	// to false so configured binaries are executed directly.
	StartByShell bool `mapstructure:"start_by_shell"`
}

// Config defines configuration for the Splunk scripted-input receiver.
type Config struct {
	// Framework is the component ID of the splunkframeworkextension to use.
	Framework component.ID `mapstructure:"framework"`

	// Scripts are rendered into in-memory [script://...] inputs.conf stanzas.
	Scripts []ScriptConfig `mapstructure:"scripts"`

	// InputsConf is optional raw inputs.conf stanza text. Use this for native
	// exec inputs that do not fit the Scripts helper, including modular inputs
	// such as [my_scheme] and [my_scheme://name].
	InputsConf string `mapstructure:"inputs_conf"`

	// PropsConf is optional raw props.conf stanza text passed to the native
	// pipeline before start.
	PropsConf string `mapstructure:"props_conf"`
}

func (c *Config) Validate() error {
	if c.Framework == (component.ID{}) {
		return fmt.Errorf("splunkexecreceiver: framework must not be empty; set it to the extension id (e.g. splunkframework)")
	}
	if len(c.Scripts) == 0 && strings.TrimSpace(c.InputsConf) == "" {
		return fmt.Errorf("splunkexecreceiver: at least one script or inputs_conf stanza must be configured")
	}
	for i, script := range c.Scripts {
		if strings.TrimSpace(script.Command) == "" {
			return fmt.Errorf("splunkexecreceiver: scripts[%d].command must not be empty", i)
		}
		if containsConfSyntaxBreak(script.Command) {
			return fmt.Errorf("splunkexecreceiver: scripts[%d].command contains unsupported conf stanza characters", i)
		}
		fields := map[string]string{
			"interval":   script.Interval,
			"sourcetype": script.Sourcetype,
			"index":      script.Index,
			"host":       script.Host,
			"source":     script.Source,
		}
		for name, value := range fields {
			if containsConfLineBreak(value) {
				return fmt.Errorf("splunkexecreceiver: scripts[%d].%s contains a line break", i, name)
			}
		}
	}
	return nil
}

func containsConfSyntaxBreak(value string) bool {
	return strings.ContainsAny(value, "\r\n[]")
}

func containsConfLineBreak(value string) bool {
	return strings.ContainsAny(value, "\r\n")
}
