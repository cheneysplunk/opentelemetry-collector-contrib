// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package pcpumetrics defines the Recorder interface used by processors to
// emit per-CPU-usage metrics.  Phase 2 ships only the no-op implementation;
// the live meter-backed recorder is wired in Phase 6.
package pcpumetrics

// Recorder collects and emits per-source / per-sourcetype CPU-usage metrics.
// Implementations must be safe for concurrent use.
type Recorder interface {
	// Close flushes any pending metrics and releases resources.
	Close() error
}

type nopRecorder struct{}

func (nopRecorder) Close() error { return nil }

// NewNop returns a Recorder that discards all observations.
func NewNop() Recorder { return nopRecorder{} }
