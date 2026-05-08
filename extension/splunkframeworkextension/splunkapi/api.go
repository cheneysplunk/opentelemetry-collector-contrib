// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package splunkapi defines the pure-Go interfaces that splunkframeworkextension
// exposes to other components. No CGo, no Splunk headers — import freely on any
// platform; only the Linux extension implementation uses CGo.
//
// Usage in a receiver or exporter:
//
//	func (r *myReceiver) Start(_ context.Context, host component.Host) error {
//	    var fw splunkapi.SplunkFramework
//	    for _, ext := range host.GetExtensions() {
//	        if f, ok := ext.(splunkapi.SplunkFramework); ok {
//	            fw = f; break
//	        }
//	    }
//	    if fw == nil {
//	        return errors.New("splunkframeworkextension not found in service.extensions")
//	    }
//	    ti, err := fw.NewTailInput(splunkapi.TailConfig{...})
//	    ...
//	}
package splunkapi

import "time"

// SplunkFramework is implemented by splunkframeworkextension.
// Components retrieve it by iterating host.GetExtensions() and
// type-asserting to SplunkFramework.
type SplunkFramework interface {
	// NewTailInput creates a tail-input session backed by the Splunk tail
	// pipeline (fish-bucket, log-rotation, line-breaking, etc.).
	// Multiple TailInputs can coexist within one process.
	NewTailInput(cfg TailConfig) (TailInput, error)

	// NewTcpOutput creates an S2S TCP output session to a Splunk indexer.
	// Multiple TcpOutputs can coexist (one per indexer target).
	NewTcpOutput(host string, port int, index string) (TcpOutput, error)
}

// TailConfig is the top-level configuration for a TailInput session.
type TailConfig struct {
	// DefaultSourcetype applied to monitors that don't specify one.
	// Empty → "tailin".
	DefaultSourcetype string

	// DefaultIndex applied to monitors that don't specify one.
	// Empty → "main".
	DefaultIndex string

	// Host field value for all events. Empty → local hostname.
	Host string

	// FishbucketDir is the path to a writable directory for fish-bucket
	// state files. Empty → $SPLUNK_DB/fishbucket.
	FishbucketDir string
}

// MonitorConfig describes one file-glob stanza to watch.
type MonitorConfig struct {
	// Glob is a shell-style glob or exact path, e.g. "/var/log/*.log".
	Glob string

	// Sourcetype overrides TailConfig.DefaultSourcetype for this glob.
	// Empty → inherit default.
	Sourcetype string

	// Index overrides TailConfig.DefaultIndex for this glob.
	// Empty → inherit default.
	Index string

	// Host overrides TailConfig.Host for this glob.
	// Empty → inherit default.
	Host string
}

// TailEvent is a single log event delivered by the Splunk tail pipeline.
type TailEvent struct {
	Body       string
	Source     string
	Sourcetype string
	Host       string
	Time       time.Time
}

// TailInput is a running tail session.
type TailInput interface {
	// AddMonitor registers a glob pattern. Must be called before Start.
	AddMonitor(cfg MonitorConfig) error

	// Start begins the tail pipeline. Must be called after AddMonitor(s).
	Start() error

	// Events returns the channel on which TailEvents are delivered.
	// The channel is closed when Stop+Destroy are called.
	Events() <-chan TailEvent

	// Stop halts the tail pipeline and all background threads.
	// No more events will be sent after Stop returns.
	// The Events channel is closed by Stop.
	Stop()

	// Destroy frees C-level resources. Must be called after Stop.
	Destroy()
}

// TcpOutput is an active S2S session to one Splunk indexer.
type TcpOutput interface {
	// Send forwards one event. Metadata fields may be empty strings.
	Send(body []byte, source, sourcetype, host, index string) error

	// Destroy drains the send queue and closes the TCP connection.
	Destroy(drainSeconds int)
}
