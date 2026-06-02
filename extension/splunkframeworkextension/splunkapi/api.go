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
//	    p, err := fw.NewMonitorPipeline()
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

	// NewMonitorPipeline creates an input pipeline from the extension-owned
	// merged inputs.conf cache. The native TailManager reads the configured
	// monitor:// stanzas; callers do not pass monitor config in memory.
	NewMonitorPipeline() (Pipeline, error)

	// NewOutputPipeline creates an output pipeline from an existing
	// outputs.conf tcpout group. The group name is the bare name, for example
	// "prod" for [tcpout:prod].
	NewOutputPipeline(outputGroup, defaultIndex string) (Pipeline, error)

	// ConfManager returns the Splunk conf/auth management API backed by the
	// native merged PropertyPages cache initialised at extension startup.
	ConfManager() (ConfManager, error)
}

// ConfManager exposes Splunk's layered .conf management and auth/authz APIs.
type ConfManager interface {
	// Get reads a single merged conf value. ok is false when the key is absent.
	Get(confName, stanza, key string) (value string, ok bool, err error)

	// Set writes a key=value into the local layer, creating the stanza if needed.
	Set(confName, stanza, key, value string) error

	// GetStanza returns all merged key=value pairs in a stanza.
	GetStanza(confName, stanza string) (map[string]string, error)

	// ListStanzas returns all stanza names in a conf file.
	ListStanzas(confName string) ([]string, error)

	// DeleteStanza removes a stanza from the local layer.
	DeleteStanza(confName, stanza string) error

	// DeleteKey removes a key from a stanza in the local layer.
	DeleteKey(confName, stanza, key string) error

	// Reload re-reads and re-merges a conf file from disk.
	Reload(confName string) error

	// Login authenticates a Splunk user and returns a session wrapper.
	Login(username, password string) (ConfSession, error)
}

// ConfSession is an authenticated Splunk conf-management session.
type ConfSession interface {
	Username() string
	Logout()
	CheckCapability(capability string) (bool, error)
	Roles() ([]string, error)
}

// PipelineConfig holds raw Splunk conf stanza text for each conf file.
// Standard Splunk .conf syntax: [stanza-header]\nkey = value\n...
type PipelineConfig struct {
	// InputsConf is raw inputs.conf stanza text. Empty for output-only.
	// Supported stanza types: monitor://, script://, and modular exec input
	// schemes such as my_scheme://name.
	// Example:
	//   [script://./bin/my_input]
	//   interval = 60
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
	// Body is the event text as a Go string (NUL-terminated copy from C).
	// Set when the pipeline was created with splunk_pipeline_create.
	// Empty when RawBody is set.
	Body string

	// RawBody holds the event bytes without a string copy or NUL termination.
	// Set when the pipeline was created with splunk_pipeline_create_bytes.
	// Preferred for callers that store events as []byte (e.g. OTel plog bytes body).
	// Empty when Body is set.
	RawBody    []byte
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

	// SendToGroup forwards one event through the named tcpout group by setting
	// Splunk's _TCP_ROUTING metadata. outputGroup is the bare group name.
	SendToGroup(body []byte, source, sourcetype, host, index, outputGroup string) error

	// Stop halts inputs (no more events after return), drains outputs
	// for up to drainSeconds, and closes the Events channel.
	Stop(drainSeconds int)

	// Destroy frees C-level resources. Must be called after Stop.
	Destroy()
}
