// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"context"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/pcpumetrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/pdataemit"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/rulecache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/chunkedlbprocessor/internal/processorshared/splunkctl"
)

// stringsIndexByte aliases strings.IndexByte (resolves to the same SIMD-
// optimized assembly as bytes.IndexByte). Wrapped to keep import surface
// explicit for the Phase 3 swap.
func stringsIndexByte(s string, c byte) int { return strings.IndexByte(s, c) }

// chunkedLBProcessor is the Logs processor implementation. Phase 2 ships:
//
//   - Default LF (`bytes.IndexByte`) boundary detection only — custom regex
//     patterns log a one-shot warning per sourcetype and pass through (no
//     PCRE2 yet; that's Phase 2.5 + 3).
//   - §9.2 ordering invariant for emitted records (firstHalf → done → tail
//     under the same ScopeLogs slice, captured-once-Len iteration).
//   - `done_key_emission` knob (always | auto | never).  Default: never.
//     No OTel-contrib exporter (including splunk_hec) currently understands
//     the marker, so the safe Phase 2 default is "do not emit"; operators
//     opt in once a marker-aware downstream is wired.  See design doc
//     §6.0.4 for the exporter compatibility matrix.
//   - `httpout_compat` short-circuit (mirrors
//     S2SOverHttpOutputProcessor::isHttpOutConfigured()).
//   - `treat_input_as_complete_events` (default false) — opt-in mirror
//     of C++ ChunkedLBProcessor::shouldProcess hasDoneKey() pass-through
//     (ChunkedLBProcessor.cpp:184). When true, every non-empty input
//     record gets splunkctl.FlagDone OR-merged in before pre-scan;
//     condition-5 then routes them through the unchanged pass-through
//     path, mirroring exactly what C++ does for CPDs that arrive with
//     hasDoneKey() == true. Required for OTel pipelines whose input
//     receiver emits one complete logical event per LogRecord (e.g.
//     filelog with multiline aggregation, journald, syslog) — without
//     this flag, splitting at the first \n would leave the last partial
//     line of one record's tail to fuse with the next record's
//     firstHalf in the indexer's per-channel LineBreaker buffer (the
//     same buffer-fusion behavior C++ avoids by virtue of its
//     parser-streamed CPDs always ending on a complete event boundary
//     OR carrying hasDoneKey() for the final read).
//   - Per-sourcetype rule cache via internal/processorshared/rulecache (v1
//     stub — sync.Map-backed, no LRU yet).
//   - pcpumetrics no-op recorder (Phase 6 wires the real meter).
//
// Pipeline placement requirement: chunkedlb MUST sit at the very end of the
// processor chain, immediately before the exporter — and at minimum AFTER
// any `attributesprocessor`, `filterprocessor`, `transformprocessor`,
// `groupbyattrsprocessor`, or `routingprocessor`.  Those processors can
// drop, reorder, or re-route the synthetic doneKey marker, breaking the
// §9.2 ordering invariant.  See design doc §6.0.4 for details.
//
// Concurrency: ConsumeLogs (which dispatches into processLogs) may be invoked
// concurrently from multiple receiver goroutines. All hot-path state must be
// either read-only or synchronized via atomics / sync.Pool / sync.Map (§7).
type chunkedLBProcessor struct {
	cfg    *Config
	logger *zap.Logger

	defaultRule *ruleEntry
	cache       *rulecache.Cache[string, *ruleEntry]
	cpuMetrics  pcpumetrics.Recorder
}

// ruleEntry is the cached, immutable per-sourcetype state. Only the
// dispatcher type (defaultLF vs custom) and the pattern string are needed in
// Phase 2; Phase 3 replaces `dispatcher` with a hybrid handle that may carry
// a PCRE2 *cgoPattern.
type ruleEntry struct {
	enabled    bool
	pattern    string
	dispatcher dispatcherKind

	// warnedCustom guards a one-shot zap warning for sourcetypes whose
	// EVENT_BREAKER is non-default. Mutated under sync.Once semantics.
	warnedCustom sync.Once
}

type dispatcherKind int

const (
	dispatcherDisabled dispatcherKind = iota
	dispatcherDefaultLF
	dispatcherCustomUnsupported // Phase 2: warn-and-passthrough
)

// defaultLFEquivalentPatterns enumerates the strict allow-list for the
// pure-Go fast path (design doc §6.0). Anything outside this set —
// even semantically equivalent — falls to the custom path.
var defaultLFEquivalentPatterns = map[string]struct{}{
	"":          {},
	`([\r\n]+)`: {},
	`[\r\n]+`:   {},
	`(\r\n|\n)`: {},
}

func newChunkedLBProcessor(settings processor.Settings, cfg *Config) (*chunkedLBProcessor, error) {
	p := &chunkedLBProcessor{
		cfg:        cfg,
		logger:     settings.Logger,
		cpuMetrics: pcpumetrics.NewNop(),
	}
	p.defaultRule = compileRule("default_rule", cfg.DefaultRule, settings.Logger)
	p.cache = rulecache.New[string, *ruleEntry](
		cfg.MaxCachedRules,
		nil, // ruleEntry has no native resources in Phase 2
	)
	return p, nil
}

func compileRule(label string, rc RuleConfig, logger *zap.Logger) *ruleEntry {
	if !rc.EventBreakerEnable {
		return &ruleEntry{enabled: false, pattern: rc.EventBreaker, dispatcher: dispatcherDisabled}
	}
	if _, ok := defaultLFEquivalentPatterns[rc.EventBreaker]; ok {
		return &ruleEntry{enabled: true, pattern: rc.EventBreaker, dispatcher: dispatcherDefaultLF}
	}
	logger.Warn("chunkedlb: complex regex EVENT_BREAKER not supported yet; rule will pass through",
		zap.String("rule", label),
		zap.String("event_breaker", sanitizeForError(rc.EventBreaker)),
	)
	return &ruleEntry{enabled: true, pattern: rc.EventBreaker, dispatcher: dispatcherCustomUnsupported}
}

func (p *chunkedLBProcessor) start(_ context.Context, _ component.Host) error {
	p.logger.Info("chunkedlb processor started",
		zap.String("phase", "2 (default-LF + doneKey, custom regex pass-through)"),
		zap.Bool("httpout_compat", p.cfg.HTTPOutCompat),
		zap.String("done_key_emission", p.cfg.DoneKeyEmission),
		zap.Int("max_cached_rules", p.cfg.MaxCachedRules),
		zap.Int("preconfigured_rules", len(p.cfg.Rules)),
	)
	return nil
}

func (p *chunkedLBProcessor) shutdown(_ context.Context) error {
	if err := p.cache.Close(); err != nil {
		return err
	}
	if err := p.cpuMetrics.Close(); err != nil {
		return err
	}
	p.logger.Info("chunkedlb processor shutdown complete")
	return nil
}

// processLogs is invoked once per ConsumeLogs call. It walks every
// ResourceLogs / ScopeLogs and may grow each ScopeLogs.LogRecords() slice in
// place (§9.2). Caller is processorhelper.NewLogs which has been configured
// with MutatesData: true.
func (p *chunkedLBProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	// Fast-exit ladder (design doc §6.0.2):
	// 1. (TODO Phase 4) global atomic kill switch
	// 2. httpout_compat → pass-through unmodified.
	if p.cfg.HTTPOutCompat {
		return ld, nil
	}

	emitMarker := p.shouldEmitMarker()

	rls := ld.ResourceLogs()
	for ri := 0; ri < rls.Len(); ri++ {
		rl := rls.At(ri)
		// Per-resource sourcetype lookup (Splunk encodes sourcetype on the
		// stream, not on the individual event — we honor the same shape and
		// only fall back to per-record lookup when the resource is silent).
		resourceSourcetype, _ := lookupSourcetype(rl.Resource().Attributes(), p.cfg.SourcetypeAttribute)

		sls := rl.ScopeLogs()
		for si := 0; si < sls.Len(); si++ {
			sl := sls.At(si)
			// treat_input_as_complete_events: stamp FlagDone on every
			// non-empty input record before the pre-scan. This mirrors
			// C++ ChunkedLBProcessor::shouldProcess hasDoneKey()
			// pass-through (ChunkedLBProcessor.cpp:184) for upstreams
			// that emit one complete logical event per LogRecord (e.g.
			// filelog with multiline aggregation). The condition-5
			// check inside classifyRecord then routes these records
			// through the unchanged pass-through path, and the
			// Splunk-aware exporter translates the FlagDone into
			// pd.SetDone() on the wire so the indexer flushes after
			// every record. See Config.TreatInputAsCompleteEvents.
			if p.cfg.TreatInputAsCompleteEvents {
				p.stampCompleteEventFlag(sl)
			}
			p.processScopeLogs(sl, resourceSourcetype, emitMarker)
		}
	}
	return ld, nil
}

// stampCompleteEventFlag walks every LogRecord in sl and OR-merges
// splunkctl.FlagDone into the canonical splunkctl payload for any record
// whose body is a non-empty string. Empty-body records are left
// untouched: an empty body either means "synthetic marker already" (and
// will already carry FlagDone via splunkctl) or "no event content"
// (which has nothing to flush). Records that already carry FlagDone are
// also no-ops thanks to OrFlags semantics.
func (p *chunkedLBProcessor) stampCompleteEventFlag(sl plog.ScopeLogs) {
	lrs := sl.LogRecords()
	for li := 0; li < lrs.Len(); li++ {
		lr := lrs.At(li)
		if lr.Body().Type() != pcommon.ValueTypeStr {
			continue
		}
		if lr.Body().Str() == "" {
			continue
		}
		splunkctl.OrFlags(lr, splunkctl.FlagDone)
	}
}

// shouldEmitMarker resolves the done_key_emission knob to a single boolean
// for this batch. v1: "auto" is conservatively treated as "never" until
// Phase 5 wires endpoint-aware behavior.
func (p *chunkedLBProcessor) shouldEmitMarker() bool {
	switch p.cfg.DoneKeyEmission {
	case DoneKeyEmissionAlways:
		return true
	case DoneKeyEmissionAuto, DoneKeyEmissionNever:
		return false
	default:
		// Validate() should have rejected invalid values; defensive default.
		return false
	}
}

func (p *chunkedLBProcessor) processScopeLogs(sl plog.ScopeLogs, resourceSourcetype string, emitMarker bool) {
	// Two-phase rewrite (design doc §9.2):
	//   1. Pre-scan: walk the original records (read-only) and produce a
	//      []SplitDecision aligned with sl.LogRecords(). No mutation, no
	//      slice growth.
	//   2. ApplyToScope: lazily rebuild sl.LogRecords() into per-record
	//      contiguous order [fh_0, marker_0?, tail_0, fh_1, marker_1?,
	//      tail_1, ...] using MoveTo (O(1) state swap per record). When no
	//      record needs splitting, ApplyToScope short-circuits and the
	//      slice is left untouched.
	//
	// This shape mirrors C++ ChunkedLBProcessor::executeMulti, where each
	// execute() call appends its 2-3 PDs sequentially into a shared
	// output queue (develop/splunk-10.4/src/input/ChunkedLBProcessor.cpp:235-319).
	// The earlier in-place AppendEmpty pattern produced the wrong global
	// shape [fh_0, fh_1, ..., marker_0, tail_0, marker_1, tail_1, ...]
	// which fused tail_N with fh_{N+1} on the indexer's per-channel
	// streaming LineBreaker (see pdataemit package doc).
	originalLen := sl.LogRecords().Len()
	if originalLen == 0 {
		return
	}

	decisions := make([]pdataemit.SplitDecision, originalLen)
	anySplits := false
	for li := 0; li < originalLen; li++ {
		lr := sl.LogRecords().At(li)
		decisions[li] = p.classifyRecord(lr, resourceSourcetype)
		if decisions[li].Split {
			anySplits = true
		}
	}
	if !anySplits {
		return
	}

	pdataemit.ApplyToScope(sl, decisions, pdataemit.SplitOptions{
		DoneKeyAttribute: p.cfg.DoneKeyAttribute,
		EmitDoneMarker:   emitMarker,
	})
}

// classifyRecord inspects a single record (read-only) and returns the
// SplitDecision the rebuild phase needs. No mutation, safe to call inside
// the pre-scan loop. Returns Split == false for any record that should
// pass through unchanged (wrong body type, empty body, idempotent
// re-entry, disabled rule, unsupported dispatcher, or no event boundary
// in the body).
func (p *chunkedLBProcessor) classifyRecord(
	lr plog.LogRecord,
	resourceSourcetype string,
) pdataemit.SplitDecision {
	// 5. Body empty / done-marker already set → pass-through (§6.0.2).
	if lr.Body().Type() != pcommon.ValueTypeStr {
		return pdataemit.SplitDecision{}
	}
	body := lr.Body().Str()
	if body == "" {
		return pdataemit.SplitDecision{}
	}
	// Idempotency check. We skip records that already carry the canonical
	// splunkctl FlagDone marker (so chained chunkedlb processors are
	// idempotent regardless of the legacy attribute name configured), and
	// also the legacy boolean attribute when the operator has configured a
	// DoneKeyAttribute (HEC-style downstreams that may stamp it upstream).
	// See design doc §3.10 for the canonical control surface.
	if splunkctl.HasFlag(lr, splunkctl.FlagDone) {
		return pdataemit.SplitDecision{}
	}
	if p.cfg.DoneKeyAttribute != "" {
		if _, hasDoneKey := lr.Attributes().Get(p.cfg.DoneKeyAttribute); hasDoneKey {
			return pdataemit.SplitDecision{}
		}
	}

	// Sourcetype resolution: prefer the per-record attribute (mirrors
	// Splunk's CLONE_SOURCETYPE override), fall back to the resource-level
	// sourcetype, then to default_rule.
	sourcetype := resourceSourcetype
	if recST, ok := lookupSourcetype(lr.Attributes(), p.cfg.SourcetypeAttribute); ok {
		sourcetype = recST
	}

	rule := p.lookupRule(sourcetype)
	if rule == nil || !rule.enabled {
		return pdataemit.SplitDecision{}
	}

	switch rule.dispatcher {
	case dispatcherDefaultLF:
		return p.classifyDefaultLF(body)
	case dispatcherCustomUnsupported:
		// Warn once per sourcetype, then pass-through. Phase 3 will route
		// this into the cgo+PCRE2 dispatcher.
		rule.warnedCustom.Do(func() {
			p.logger.Warn(
				"chunkedlb: custom EVENT_BREAKER pattern is not supported in Phase 2; "+
					"pass-through (Phase 3 will route this into the PCRE2 dispatcher)",
				zap.String("sourcetype", sanitizeForError(sourcetype)),
				zap.String("event_breaker", sanitizeForError(rule.pattern)),
			)
		})
		return pdataemit.SplitDecision{}
	case dispatcherDisabled:
		// Unreachable: the early `!rule.enabled` check above already
		// handled disabled rules. Listed here for explicit-default
		// discipline so a future code reader does not assume the early
		// return is the only path to skip splitting.
		return pdataemit.SplitDecision{}
	default:
		return pdataemit.SplitDecision{}
	}
}

// classifyDefaultLF implements the §6.0 default fast path: find the first
// '\r' or '\n' and produce a SplitDecision describing the
// [firstHalf, tail] cut. Mirrors C++ findEventBoundary boundary++
// semantics (boundary = byteAfterTerminator;
// ChunkedLBProcessor.cpp:217-220).
func (p *chunkedLBProcessor) classifyDefaultLF(body string) pdataemit.SplitDecision {
	idx := indexOfFirstCROrLFInString(body)
	if idx < 0 {
		// No boundary at all: pass-through. Mirrors C++ ChunkedLBProcessor
		// execute() (ChunkedLBProcessor.cpp:286-290): when findEventBoundary
		// returns npos, the original cpd is pushed to the output queue
		// unchanged and NO doneKey is appended.
		return pdataemit.SplitDecision{}
	}

	// boundary = idx + 1 mirrors C++ findEventBoundary
	// (ChunkedLBProcessor.cpp:217-220): boundary points to the byte AFTER
	// the terminator. C++ does NOT special-case \r\n — it splits on the
	// first \r OR \n it sees; the trailing \n becomes the first byte of
	// the tail. We preserve that exact byte-by-byte parity.
	//
	// ChunkedLB only finds ONE chunk-boundary cut for AutoLB routing —
	// both halves can still hold many un-broken events internally, so
	// the downstream indexer's LineBreakingProcessor MUST still run on
	// each half. We therefore stamp NO control flags on firstHalf or
	// tail; the only flagged record is the synthetic done-marker
	// inserted by ApplyToScope between them. See pdataemit package doc
	// and design doc §3.9.1 for the full rationale.
	return pdataemit.SplitDecision{
		Split:     true,
		FirstHalf: body[:idx+1],
		Tail:      body[idx+1:],
	}
}

// lookupRule returns the cached ruleEntry for sourcetype. Falls back to
// default_rule when no match. Thread-safe.
func (p *chunkedLBProcessor) lookupRule(sourcetype string) *ruleEntry {
	if sourcetype == "" {
		return p.defaultRule
	}
	rc, ok := p.cfg.Rules[sourcetype]
	if !ok {
		return p.defaultRule
	}
	// Charset guard: a malicious upstream could push thousands of distinct
	// keys; we already validated configured Rules at config-load, but a
	// per-record sourcetype attribute set by an unsanitized receiver is
	// also a potential bomb. Fall back to default_rule if it fails the
	// allow-list rather than booting a cache entry.
	if !sourcetypeKeyAllowed.MatchString(sourcetype) {
		return p.defaultRule
	}
	entry, err := p.cache.Get(sourcetype, func(_ rulecache.CompileContext, key string) (*ruleEntry, error) {
		return compileRule("rules."+key, rc, p.logger), nil
	})
	if err != nil {
		// ErrCapacityExceeded or ErrClosed: degrade to default_rule rather
		// than silently dropping records.
		return p.defaultRule
	}
	return entry
}

// lookupSourcetype reads the configured sourcetype attribute, returning the
// string and whether it was present and non-empty. Centralizes the common
// "missing or empty" handling.
func lookupSourcetype(m pcommon.Map, key string) (string, bool) {
	v, ok := m.Get(key)
	if !ok {
		return "", false
	}
	if v.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	s := v.Str()
	if s == "" {
		return "", false
	}
	return s, true
}

// indexOfFirstCROrLFInString returns the smallest non-negative index of '\r'
// or '\n' in s, or -1 if neither is present.
//
// Phase 2 uses strings.IndexByte (which compiles to the same SIMD assembly
// as bytes.IndexByte on amd64/arm64) to avoid the per-record []byte allocation
// that `bytes.IndexByte([]byte(s), …)` would incur on a Go string. Phase 3
// will revisit if profiling suggests the cgo path can share a buffer view.
func indexOfFirstCROrLFInString(s string) int {
	r := stringsIndexByte(s, '\r')
	n := stringsIndexByte(s, '\n')
	switch {
	case r < 0:
		return n
	case n < 0:
		return r
	case r < n:
		return r
	default:
		return n
	}
}
