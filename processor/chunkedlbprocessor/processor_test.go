// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/splunkctl"
)

// withProcessor wires up a processor instance against a fresh consumer sink
// using the supplied config (defaults are applied if cfg is nil).
func withProcessor(t *testing.T, cfg *Config) (plog.Logs, *consumertest.LogsSink, func(plog.Logs)) {
	t.Helper()
	if cfg == nil {
		cfg = createDefaultConfig().(*Config)
	}
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.LogsSink)
	settings := processortest.NewNopSettings(NewFactory().Type())
	p, err := NewFactory().CreateLogs(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })

	return plog.NewLogs(), sink, func(ld plog.Logs) {
		require.NoError(t, p.ConsumeLogs(context.Background(), ld))
	}
}

// addRecord appends a single LogRecord with the given body and optional
// (sourcetype, attribute-key/value) under one ResourceLogs/ScopeLogs.
func addRecord(t *testing.T, logs plog.Logs, body, sourcetype string, recordAttrs map[string]string) {
	t.Helper()
	rl := logs.ResourceLogs().AppendEmpty()
	if sourcetype != "" {
		rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, sourcetype)
	}
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	for k, v := range recordAttrs {
		lr.Attributes().PutStr(k, v)
	}
}

// firstScopeRecords returns LogRecords of the first ScopeLogs of the first
// ResourceLogs of the first batch in the sink — the common shape for these
// tests.
func firstScopeRecords(t *testing.T, sink *consumertest.LogsSink) plog.LogRecordSlice {
	t.Helper()
	all := sink.AllLogs()
	require.Len(t, all, 1)
	return all[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
}

// ─────────────────────────── pass-through paths ───────────────────────────

func TestProcessor_PassThroughWhenHTTPOutCompat(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.HTTPOutCompat = true
	logs, sink, send := withProcessor(t, cfg)

	addRecord(t, logs, "line1\nline2", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len(), "httpout_compat must short-circuit")
	require.Equal(t, "line1\nline2", got.At(0).Body().Str())
}

func TestProcessor_PassThroughWhenBodyEmpty(t *testing.T) {
	logs, sink, send := withProcessor(t, nil)
	addRecord(t, logs, "", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len())
	require.Empty(t, got.At(0).Body().Str())
}

func TestProcessor_PassThroughWhenDoneKeyAlreadySet(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	logs, sink, send := withProcessor(t, cfg)

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "syslog")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("line1\nline2")
	lr.Attributes().PutBool(defaultDoneKeyAttribute, true)

	send(logs)
	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len(), "done_key already set must skip processing")
}

func TestProcessor_PassThroughWhenSplunkctlDoneAlreadySet(t *testing.T) {
	// A chained chunkedlb (or any future Splunk-aware processor) might
	// stamp the canonical splunkctl FlagDone payload. We must respect it
	// even if the operator never configured a legacy DoneKeyAttribute on
	// this processor instance.
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyAttribute = "" // legacy attribute disabled — only splunkctl
	logs, sink, send := withProcessor(t, cfg)

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "syslog")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("line1\nline2")
	splunkctl.SetFlags(lr, splunkctl.FlagDone)

	send(logs)
	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len(),
		"splunkctl FlagDone already set must skip processing (idempotent across chained chunkedlb)")
}

func TestProcessor_PassThroughWhenRuleDisabled(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DefaultRule.EventBreakerEnable = false
	logs, sink, send := withProcessor(t, cfg)

	addRecord(t, logs, "line1\nline2", "any", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len())
	require.Equal(t, "line1\nline2", got.At(0).Body().Str())
}

func TestProcessor_PassThroughWhenNoBoundary(t *testing.T) {
	logs, sink, send := withProcessor(t, nil)
	addRecord(t, logs, "no_newline_here", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len())
	require.Equal(t, "no_newline_here", got.At(0).Body().Str())
	_, hasDoneKey := got.At(0).Attributes().Get(defaultDoneKeyAttribute)
	require.False(t, hasDoneKey, "no boundary → no done-key marker on the original record")
}

// ─────────────────────────── successful split paths ───────────────────────────

func TestProcessor_DefaultLF_SplitsAndEmitsDoneMarker(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways // default is `never`; opt in for marker assertions
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "line1\nline2", "syslog", map[string]string{"app": "myapp"})
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 3, got.Len(), "expected [firstHalf, doneMarker, tail]")

	// firstHalf is the original record (mutated). It MUST NOT carry any
	// "linebreaker_done" / `_linebreaker` flag — ChunkedLB only finds ONE
	// chunk-boundary cut for AutoLB routing; the firstHalf can still hold
	// many un-broken events internally and the downstream indexer's
	// LineBreaker still has to run. See processor.go classifyDefaultLF
	// and design doc §3.9.
	firstHalf := got.At(0)
	require.Equal(t, "line1\n", firstHalf.Body().Str())
	app0, ok := firstHalf.Attributes().Get("app")
	require.True(t, ok)
	require.Equal(t, "myapp", app0.Str(), "firstHalf retains its original attributes")
	_, hasDoneOnFH := firstHalf.Attributes().Get(defaultDoneKeyAttribute)
	require.False(t, hasDoneOnFH, "firstHalf must NOT carry the done-key flag")

	marker := got.At(1)
	require.Empty(t, marker.Body().Str())
	// LEGACY: boolean attribute under the configured name (HEC-style).
	mv, ok := marker.Attributes().Get(defaultDoneKeyAttribute)
	require.True(t, ok)
	require.True(t, mv.Bool())
	// CANONICAL: splunkctl FlagDone bytes payload (always present on the
	// marker regardless of legacy attribute config). This is what
	// splunks2sexporter (and any future Splunk-aware exporter) consumes.
	require.True(t, splunkctl.HasFlag(marker, splunkctl.FlagDone),
		"marker MUST carry canonical splunkctl FlagDone payload")

	tail := got.At(2)
	require.Equal(t, "line2", tail.Body().Str())
	app, ok := tail.Attributes().Get("app")
	require.True(t, ok)
	require.Equal(t, "myapp", app.Str(), "tail must inherit non-flag attributes")
	_, hasDoneOnTail := tail.Attributes().Get(defaultDoneKeyAttribute)
	require.False(t, hasDoneOnTail, "tail must NOT carry the done-key flag")
}

func TestProcessor_DefaultLF_EmissionAlwaysVsNever(t *testing.T) {
	for _, tc := range []struct {
		name     string
		emission string
		wantLen  int
	}{
		{"always", DoneKeyEmissionAlways, 3},
		{"never", DoneKeyEmissionNever, 2},
		{"auto_treated_as_never_in_v1", DoneKeyEmissionAuto, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.DoneKeyEmission = tc.emission
			logs, sink, send := withProcessor(t, cfg)
			addRecord(t, logs, "a\nb", "syslog", nil)
			send(logs)
			got := firstScopeRecords(t, sink)
			require.Equal(t, tc.wantLen, got.Len())
		})
	}
}

func TestProcessor_DefaultLF_BoundaryAtEndStillEmitsMarker(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "complete_line\n", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 3, got.Len(), "boundary at end → firstHalf + marker + (empty) tail")
	require.Equal(t, "complete_line\n", got.At(0).Body().Str())
	require.Empty(t, got.At(1).Body().Str())
	require.Empty(t, got.At(2).Body().Str(), "tail of a boundary-at-end split is empty")
}

func TestProcessor_DefaultLF_CRBeforeLFSplitsAtCR(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "ab\r\ncd", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 3, got.Len())
	require.Equal(t, "ab\r", got.At(0).Body().Str(),
		"Phase 2 conservatively splits at the first \\r; \\n stays as the head of the tail")
	require.Equal(t, "\ncd", got.At(2).Body().Str())
}

// ─────────────────────────── per-sourcetype routing ───────────────────────────

func TestProcessor_PerSourcetypeRuleUsedOverDefault(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Rules = map[string]RuleConfig{
		"syslog": {EventBreakerEnable: false}, // disabled for this sourcetype
	}
	logs, sink, send := withProcessor(t, cfg)

	addRecord(t, logs, "line1\nline2", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len(), "per-sourcetype rule disabled the breaker")
}

func TestProcessor_RecordLevelSourcetypeOverridesResource(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Rules = map[string]RuleConfig{
		"override_st": {EventBreakerEnable: false},
	}
	logs, sink, send := withProcessor(t, cfg)

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "syslog")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("a\nb")
	lr.Attributes().PutStr(defaultSourcetypeAttribute, "override_st") // record-level wins

	send(logs)
	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len(), "record-level sourcetype override applied")
}

func TestProcessor_UnknownSourcetypeFallsBackToDefault(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways // assert marker is emitted by the fallback default rule
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "x\ny", "novel_sourcetype_42", nil)
	send(logs)
	got := firstScopeRecords(t, sink)
	require.Equal(t, 3, got.Len(), "unknown sourcetype → default_rule (which splits)")
}

// ─────────────────────────── custom regex warn-and-passthrough ─────────────

func TestProcessor_CustomRegexPattern_WarnsOnceAndPassesThrough(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Rules = map[string]RuleConfig{
		"json_app": {
			EventBreakerEnable: true,
			EventBreaker:       `(?<=\})\s*(?=\{)`, // a non-default pattern
		},
	}
	logs := plog.NewLogs()

	core, recorded := observer.New(zap.WarnLevel)
	settings := processortest.NewNopSettings(NewFactory().Type())
	settings.Logger = zap.New(core)

	sink := new(consumertest.LogsSink)
	require.NoError(t, cfg.Validate())
	p, err := NewFactory().CreateLogs(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })

	// Trigger the rule three times — warning must fire exactly once per
	// sourcetype regardless of how many records we run through.
	for i := 0; i < 3; i++ {
		addRecord(t, logs, "{a}{b}", "json_app", nil)
	}
	require.NoError(t, p.ConsumeLogs(context.Background(), logs))

	all := sink.AllLogs()
	require.Len(t, all, 1)
	require.Equal(t, 3, all[0].LogRecordCount(), "custom pattern → pass-through")
	for _, lr := range []plog.LogRecord{
		all[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0),
		all[0].ResourceLogs().At(1).ScopeLogs().At(0).LogRecords().At(0),
		all[0].ResourceLogs().At(2).ScopeLogs().At(0).LogRecords().At(0),
	} {
		require.Equal(t, "{a}{b}", lr.Body().Str())
		_, hasDoneKey := lr.Attributes().Get(defaultDoneKeyAttribute)
		require.False(t, hasDoneKey, "custom-regex passthrough must not stamp done-key on the original record")
	}

	// Count "Phase 2" warnings — both the compile-time and per-record warn
	// share the prefix; we want exactly one runtime warning per sourcetype.
	runtimeWarns := 0
	for _, e := range recorded.All() {
		if e.Message == "chunkedlb: custom EVENT_BREAKER pattern is not supported in Phase 2; "+
			"pass-through (Phase 3 will route this into the PCRE2 dispatcher)" {
			runtimeWarns++
		}
	}
	require.Equal(t, 1, runtimeWarns, "warn-once-per-sourcetype invariant")
}

// ─────────────────────────── §9.2 ordering across multiple records ─────────

func TestProcessor_MultipleRecordsInSameScopeOrderingPreserved(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways // §9.2 ordering across split + marker is the test focus
	logs, sink, send := withProcessor(t, cfg)

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "syslog")
	sl := rl.ScopeLogs().AppendEmpty()
	for _, body := range []string{"a-fh\na-tl", "b-fh\nb-tl"} {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(body)
	}

	send(logs)

	got := firstScopeRecords(t, sink)
	// §9.2 (post-fix): each source record must be emitted as a contiguous
	// [firstHalf, doneKey marker, tail] triple before the next record's
	// triple begins.  AppendEmpty()-based emission used to globally append
	// markers/tails after all firstHalves, producing [a-fh, b-fh, a-mk,
	// a-tl, b-mk, b-tl] which broke the per-channel done-key invariant
	// the indexer's LineBreakingProcessor relies on (see
	// pdataemit.ApplyToScope docs).
	require.Equal(t, 6, got.Len(),
		"expected [a-fh, a-marker, a-tl, b-fh, b-marker, b-tl] under §9.2")
	require.Equal(t, "a-fh\n", got.At(0).Body().Str())
	require.Empty(t, got.At(1).Body().Str())
	require.Equal(t, "a-tl", got.At(2).Body().Str())
	require.Equal(t, "b-fh\n", got.At(3).Body().Str())
	require.Empty(t, got.At(4).Body().Str())
	require.Equal(t, "b-tl", got.At(5).Body().Str())
}

// ─────────────────────── treat_input_as_complete_events ──────────────────

func TestProcessor_TreatInputAsCompleteEvents_StampsFlagDoneAndPassesThrough(t *testing.T) {
	// C++ ChunkedLBProcessor::shouldProcess hasDoneKey() pass-through
	// (ChunkedLBProcessor.cpp:184) for OTel pipelines whose input is
	// "one complete logical event per LogRecord" (filelog with
	// multiline, journald, syslog, ...). Every non-empty input record
	// must come out unchanged in body, BUT carry the canonical
	// splunkctl FlagDone payload so the Splunk-aware exporter
	// translates that to pd.SetDone() on the wire and the indexer's
	// LineBreakingProcessor flushes after every record.
	cfg := createDefaultConfig().(*Config)
	cfg.TreatInputAsCompleteEvents = true
	logs, sink, send := withProcessor(t, cfg)

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "syslog")
	sl := rl.ScopeLogs().AppendEmpty()
	for _, body := range []string{
		"=== CHUNK 1 headline\nline-a-1\nline-b-1\nline-c-1",
		"=== CHUNK 2 headline\nline-a-2\nline-b-2\nline-c-2",
	} {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(body)
	}
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 2, got.Len(),
		"treat_input_as_complete_events MUST short-circuit the split: each input record passes through 1:1, NOT [fh,marker,tail]")

	for i, want := range []string{
		"=== CHUNK 1 headline\nline-a-1\nline-b-1\nline-c-1",
		"=== CHUNK 2 headline\nline-a-2\nline-b-2\nline-c-2",
	} {
		require.Equal(t, want, got.At(i).Body().Str(),
			"record %d body must be preserved verbatim — chunkedlb stamps FlagDone and passes through, mirroring C++ hasDoneKey() branch", i)
		require.True(t, splunkctl.HasFlag(got.At(i), splunkctl.FlagDone),
			"record %d MUST carry splunkctl FlagDone so the Splunk-aware exporter sets pd.SetDone() on the wire", i)
	}
}

func TestProcessor_TreatInputAsCompleteEvents_EmptyBodyIsNotStamped(t *testing.T) {
	// Empty-body records either carry FlagDone already (synthetic
	// marker from a chained chunkedlb) or have nothing to flush. Either
	// way, OrFlags would be a no-op for the marker case — and stamping
	// FlagDone on a truly content-less record produces a misleading
	// non-marker non-data PD. Skip them.
	cfg := createDefaultConfig().(*Config)
	cfg.TreatInputAsCompleteEvents = true
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 1, got.Len())
	require.Empty(t, got.At(0).Body().Str())
	require.False(t, splunkctl.HasFlag(got.At(0), splunkctl.FlagDone),
		"empty-body input must not be stamped with FlagDone")
}

func TestProcessor_TreatInputAsCompleteEvents_DefaultFalsePreservesSplitting(t *testing.T) {
	// Strict-streaming C++ semantics is the default. With the flag off,
	// chunkedlb still finds the first \r/\n and splits into [fh, tail]
	// (plus optional marker). This is the legacy behavior any operator
	// who isn't explicitly opting in still gets.
	cfg := createDefaultConfig().(*Config)
	require.False(t, cfg.TreatInputAsCompleteEvents,
		"default value must be false to preserve legacy/streaming semantics")
	cfg.DoneKeyEmission = DoneKeyEmissionAlways
	logs, sink, send := withProcessor(t, cfg)
	addRecord(t, logs, "head\ntail", "syslog", nil)
	send(logs)

	got := firstScopeRecords(t, sink)
	require.Equal(t, 3, got.Len(),
		"with the flag off, chunkedlb still splits [head, marker, tail]")
	require.Equal(t, "head\n", got.At(0).Body().Str())
	require.True(t, splunkctl.HasFlag(got.At(1), splunkctl.FlagDone))
	require.Equal(t, "tail", got.At(2).Body().Str())
}

// ─────────────────────────── helper coverage ───────────────────────────

func TestIndexOfFirstCROrLF(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", -1},
		{"none", "abcdef", -1},
		{"only_lf", "abc\ndef", 3},
		{"only_cr", "abc\rdef", 3},
		{"cr_before_lf", "ab\r\ncd", 2},
		{"lf_before_cr", "ab\n\rcd", 2},
		{"first_byte", "\nabc", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, indexOfFirstCROrLFInString(tc.in))
		})
	}
}
