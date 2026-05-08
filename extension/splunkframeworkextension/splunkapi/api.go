// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package splunkapi defines the pure-Go interfaces exposed by
// splunkframeworkextension to other OTel components. No CGo, no Splunk
// headers — import freely on any platform.
//
// Usage:
//
//	func (r *myReceiver) Start(_ context.Context, host component.Host) error {
//	    var fw splunkapi.SplunkFramework
//	    for _, ext := range host.GetExtensions() {
//	        if f, ok := ext.(splunkapi.SplunkFramework); ok { fw = f; break }
//	    }
//	    p, err := fw.NewPipeline(splunkapi.PipelineConfig{
//	        InputsConf: "[monitor:///var/log/app/*.log]\nsourcetype=myapp\n",
//	    })
//	    ...
//	}
package splunkapi

import "time"

// SplunkFramework is implemented by splunkframeworkextension.
// Retrieve it by iterating host.GetExtensions() and type-asserting.
type SplunkFramework interface {
	// NewPipeline creates a Splunk pipeline from raw conf stanza text.
	// Either InputsConf or OutputsConf (or both) must be non-empty.
	NewPipeline(cfg PipelineConfig) (Pipeline, error)
}

// PipelineConfig holds raw Splunk conf stanza text for each conf file.
// Standard Splunk .conf syntax: [stanza-header]\nkey = value\n...
type PipelineConfig struct {
	// InputsConf is raw inputs.conf stanza text. Empty for output-only.
	// Supported stanza types (Phase 1): monitor://
	// Example:
	//   [monitor:///var/log/*.log]
	//   sourcetype = myapp
	//   index = main
	InputsConf string

	// OutputsConf is raw outputs.conf stanza text. Empty for input-only.
	// Example:
	//   [tcpout]
	//   defaultGroup = idx
	//   [tcpout:idx]
	//   server = 10.0.0.1:9997
	OutputsConf string

	// PropsConf is raw props.conf stanza text. Empty for defaults.
	// Reserved for Phase 2.
	PropsConf string
}

// Event is a single log event delivered from an input pipeline.
type Event struct {
	Body       string
	Source     string
	Sourcetype string
	Host       string
	Time       time.Time
}

// Pipeline is a running Splunk pipeline session (input, output, or both).
type Pipeline interface {
	// Start activates the pipeline. For inputs, events begin arriving via
	// Events(). For outputs, the TCP connection is established.
	Start() error

	// Events returns the channel on which input Events are delivered.
	// Returns a nil channel for output-only pipelines.
	// The channel is closed when Stop is called.
	Events() <-chan Event

	// Send forwards one event through the output side of the pipeline.
	// Returns an error for input-only pipelines or after Stop is called.
	Send(body []byte, source, sourcetype, host, index string) error

	// Stop halts inputs (no more events after return), drains outputs
	// for up to drainSeconds, and closes the Events channel.
	Stop(drainSeconds int)

	// Destroy frees C-level resources. Must be called after Stop.
	Destroy()
}

