// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package splunkctl defines the canonical control-plane attribute for
// Splunk-aware OTel processors and exporters.
//
// Control flags are stored as a single int64 attribute on the LogRecord under
// the key "com.splunk.ctl".  Processors OR flags into it; exporters test for
// specific flags to decide how to translate records on the wire.
package splunkctl

import "go.opentelemetry.io/collector/pdata/plog"

const ctlAttrKey = "com.splunk.ctl"

// Flag is a bitmask of control signals carried on a LogRecord.
type Flag int64

// FlagDone signals that the record represents a complete event boundary.
// Mirrors the C++ donePD / pd.setDone() semantic: the downstream
// LineBreakingProcessor (on the indexer side) should flush its buffer at this
// boundary.
const FlagDone Flag = 1 << 0

// HasFlag reports whether lr carries all of the bits in flag.
func HasFlag(lr plog.LogRecord, flag Flag) bool {
	v, ok := lr.Attributes().Get(ctlAttrKey)
	if !ok {
		return false
	}
	return Flag(v.Int())&flag == flag
}

// OrFlags OR-merges flag into the existing ctl attribute on lr, creating the
// attribute if it does not yet exist.
func OrFlags(lr plog.LogRecord, flag Flag) {
	existing := Flag(0)
	if v, ok := lr.Attributes().Get(ctlAttrKey); ok {
		existing = Flag(v.Int())
	}
	lr.Attributes().PutInt(ctlAttrKey, int64(existing|flag))
}

// SetFlags replaces the ctl attribute on lr with flag (discarding any
// previously OR-ed flags).
func SetFlags(lr plog.LogRecord, flag Flag) {
	lr.Attributes().PutInt(ctlAttrKey, int64(flag))
}
