// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package pdataemit provides helpers for rewriting pdata LogRecord slices in
// place according to ChunkedLB split decisions (design doc §9.2).
//
// # Ordering invariant
//
// When ApplyToScope rewrites a ScopeLogs, the output record ordering is:
//
//	[fh_0, marker_0?, tail_0, fh_1, marker_1?, tail_1, …]
//
// Every group (firstHalf, optional marker, tail) is contiguous under the
// same ScopeLogs slice so that downstream exporters and the indexer's
// per-channel LineBreakingProcessor see each group as an ordered unit.
// The marker (when emitted) carries splunkctl.FlagDone; downstream Splunk-
// aware exporters translate it to pd.setDone() on the wire PD.
//
// Records whose SplitDecision.Split is false are copied to the output
// unchanged (pass-through).
package pdataemit

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/splunkctl"
)

// SplitDecision describes how a single LogRecord should be handled.
type SplitDecision struct {
	// Split is true when the record body contains an event boundary and
	// should be rewritten into [firstHalf, marker?, tail].
	Split bool

	// FirstHalf is the body prefix up to and including the boundary terminator
	// (e.g. "line1\n").  Only meaningful when Split is true.
	FirstHalf string

	// Tail is the body suffix after the boundary terminator (e.g. "line2").
	// Only meaningful when Split is true.
	Tail string
}

// SplitOptions controls optional behaviour in ApplyToScope.
type SplitOptions struct {
	// DoneKeyAttribute, if non-empty, causes a legacy boolean attribute with
	// this key to be set to true on the synthetic done marker.  Kept for
	// HEC-style downstreams that key off a string attribute rather than the
	// canonical splunkctl payload.
	DoneKeyAttribute string

	// EmitDoneMarker controls whether a synthetic marker LogRecord is
	// inserted between firstHalf and tail when a split is applied.
	EmitDoneMarker bool
}

// ApplyToScope rewrites sl.LogRecords() in-place according to decisions.
// decisions must be aligned with sl.LogRecords(): decisions[i] describes
// the record at index i.  Only called when at least one decision has Split==true.
//
// The caller (chunkedLBProcessor.processScopeLogs) already verified that
// len(decisions) == sl.LogRecords().Len() and that anySplits is true before
// calling this function.
func ApplyToScope(sl plog.ScopeLogs, decisions []SplitDecision, opts SplitOptions) {
	origLen := len(decisions)
	if origLen == 0 {
		return
	}

	// Count total output records to pre-allocate the staging slice.
	total := 0
	for _, d := range decisions {
		if d.Split {
			total += 2 // firstHalf + tail
			if opts.EmitDoneMarker {
				total++ // marker between them
			}
		} else {
			total++
		}
	}

	// Build the output sequence in a staging ScopeLogs.  Using a staging
	// area lets us build in the correct [fh, marker?, tail] order without
	// fighting pdata's append-only LogRecordSlice API.
	staging := plog.NewScopeLogs()
	staging.LogRecords().EnsureCapacity(total)

	lrs := sl.LogRecords()
	for i, d := range decisions {
		lr := lrs.At(i)
		if !d.Split {
			// Pass-through: copy the record verbatim.
			lr.CopyTo(staging.LogRecords().AppendEmpty())
			continue
		}

		// ── firstHalf ──────────────────────────────────────────────────────
		// Copy all fields from the original record (timestamps, attributes,
		// resource, etc.) then overwrite the body with the firstHalf slice.
		fh := staging.LogRecords().AppendEmpty()
		lr.CopyTo(fh)
		fh.Body().SetStr(d.FirstHalf)

		// ── done marker (optional) ─────────────────────────────────────────
		if opts.EmitDoneMarker {
			marker := staging.LogRecords().AppendEmpty()
			// Marker body is empty — it is purely a control record.
			marker.Body().SetStr("")
			// Legacy attribute: kept for HEC-style downstreams.
			if opts.DoneKeyAttribute != "" {
				marker.Attributes().PutBool(opts.DoneKeyAttribute, true)
			}
			// Canonical control payload: always present on the marker so
			// Splunk-aware exporters can translate it to pd.setDone().
			splunkctl.SetFlags(marker, splunkctl.FlagDone)
		}

		// ── tail ───────────────────────────────────────────────────────────
		// Copy all fields from the original record then overwrite the body
		// with the tail slice.  Attributes are inherited so that resource-
		// level sticky-routing keys (e.g. _splunk_channel) flow through.
		tail := staging.LogRecords().AppendEmpty()
		lr.CopyTo(tail)
		tail.Body().SetStr(d.Tail)
	}

	// Replace sl.LogRecords() with the correctly ordered staging content.
	sl.LogRecords().RemoveIf(func(plog.LogRecord) bool { return true })
	staging.LogRecords().MoveAndAppendTo(sl.LogRecords())
}
