// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package chunkedlbprocessor implements an OTel Logs processor that ports the
// semantics of Splunk's C++ ChunkedLBProcessor
// (develop/splunk-10.4/src/input/ChunkedLBProcessor.{h,cpp}).
//
// See docs/chunkedlb-otel-processor-design.md for the full design, including
// the hybrid Go/cgo dispatcher, the §6.1 bridge contract, the §9.2 ordering
// invariant, and the security/limits hardening rules.
package chunkedlbprocessor

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Config is the top-level processor configuration.  Field naming and defaults
// are derived from props.conf.spec / limits.conf.spec in develop/splunk-10.4
// (see docs/chunkedlb-otel-processor-design.md §3 for the full mapping).
type Config struct {
	// SourcetypeAttribute is the LogRecord attribute key used to select the
	// per-sourcetype Rule.  Defaults to "com.splunk.sourcetype" — the same
	// key the splunk_hec exporter uses.
	SourcetypeAttribute string `mapstructure:"sourcetype_attribute"`

	// DefaultRule is applied when a LogRecord either has no SourcetypeAttribute
	// or has a value not present in Rules.  Equivalent to Splunk's [default]
	// stanza.
	DefaultRule RuleConfig `mapstructure:"default_rule"`

	// Rules maps sourcetype string → per-sourcetype rule.  This is the OTel
	// equivalent of the per-sourcetype props.conf stanzas that govern
	// EVENT_BREAKER / EVENT_BREAKER_ENABLE in Splunk.
	Rules map[string]RuleConfig `mapstructure:"rules"`

	// CPUProfiling mirrors limits.conf 'clb_cpu_profiling'.  When true the
	// processor emits per_host / per_source / per_sourcetype / per_index
	// CPU-usage metrics via the OTel meter (see plan §6 / §6.0.3).
	CPUProfiling bool `mapstructure:"cpu_profiling"`

	// HTTPOutCompat mirrors S2SOverHttpOutputProcessor::isHttpOutConfigured()
	// from develop/splunk-10.4/src/input/ChunkedLBProcessor.cpp line 99.
	// When true the processor short-circuits to pass-through, matching the
	// behavior Splunk uses to avoid event-breaking when [httpout] is configured.
	// There is no global httpout state in OTel, so this is an explicit toggle.
	HTTPOutCompat bool `mapstructure:"httpout_compat"`

	// DoneKeyAttribute names the LEGACY string-attribute used to mark the
	// synthetic "done" record emitted between firstHalf and tail when a
	// body is split. Mirrors donePD.createDoneKeyFrom(cpd) from the C++
	// reference.
	//
	// As of the splunkctl migration (design doc §3.10), the marker
	// ALWAYS carries the canonical splunkctl FlagDone payload regardless
	// of this field. DoneKeyAttribute is now purely additive: when
	// non-empty, the marker also gets a boolean attribute under that key
	// (kept for HEC-style downstreams that may already key off it). Set
	// to empty string to suppress the legacy attribute entirely.
	//
	// The "already-marked" pass-through check honors BOTH the splunkctl
	// payload (for chained Splunk-aware processors) AND the legacy
	// attribute when this field is non-empty.
	DoneKeyAttribute string `mapstructure:"done_key_attribute"`

	// Regex bundles all PCRE2 limits + DoS hardening knobs.  See §8 of the
	// design doc for the threat model these are guarding against.
	Regex RegexLimits `mapstructure:"regex"`

	// MaxCachedRules caps the number of distinct sourcetypes that get a cached
	// compiled pattern.  LRU-evicted beyond this.  Plan §8: prevents
	// map-bombing with attacker-controlled sourcetype values.
	MaxCachedRules int `mapstructure:"max_cached_rules"`

	// DoneKeyEmission controls when the synthetic done-marker LogRecord is
	// inserted between the firstHalf and tail of a split body.  Mirrors
	// docs/chunkedlb-otel-processor-design.md §3 and §9.3:
	//
	//   "always" (default) — emit on every successful boundary split.
	//   "auto"             — emit only when an upstream attribute hints
	//                        the marker will be functional downstream.
	//                        v1 reads no upstream hint, so "auto" is
	//                        currently equivalent to "never"; reserved
	//                        for Phase 5 endpoint-aware behavior.
	//   "never"            — never emit; ChunkedLB still splits the body
	//                        but does not insert the sentinel.
	DoneKeyEmission string `mapstructure:"done_key_emission"`

	// TreatInputAsCompleteEvents mirrors the C++ ChunkedLBProcessor::
	// shouldProcess hasDoneKey() pass-through branch
	// (develop/splunk-10.4/src/input/ChunkedLBProcessor.cpp:184) for the
	// case where the upstream is NOT a streaming byte-reader but a
	// receiver that emits one complete logical event per LogRecord
	// (filelog with multiline aggregation, journald, syslog, ...).
	//
	// When true, every non-empty-body input record is stamped with the
	// canonical splunkctl FlagDone before the per-record split decision
	// runs. The existing condition-5 check
	// (`splunkctl.HasFlag(lr, FlagDone)`) then routes the record through
	// the unchanged pass-through path, exactly mirroring what C++ does
	// for CPDs that arrive with hasDoneKey() == true. The Splunk-aware
	// exporter (splunks2sexporter with splunkctl_compat=true) translates
	// the FlagDone into pd.SetDone() on the wire PD, so the indexer's
	// LineBreakingProcessor flushes its per-channel buffer at the end of
	// every record — preventing fusion between successive complete
	// events.
	//
	// Default false preserves strict-streaming C++ semantics for any
	// hypothetical upstream that DOES produce partial-event byte chunks
	// (where ChunkedLB's split-at-first-LF behavior is the right thing).
	//
	// Set to true for ALL OTel pipelines whose input is a "one logical
	// event per LogRecord" receiver. See design doc §6.0.5 (added with
	// this option) for the receiver compatibility table.
	TreatInputAsCompleteEvents bool `mapstructure:"treat_input_as_complete_events"`
}

// RuleConfig holds the per-sourcetype event-breaking rules.  Field names are
// the OTel equivalents of the props.conf keys listed in plan §3.
type RuleConfig struct {
	// EventBreakerEnable mirrors props.conf 'EVENT_BREAKER_ENABLE'.
	// When false the rule is a no-op pass-through for matching events.
	EventBreakerEnable bool `mapstructure:"event_breaker_enable"`

	// EventBreaker is the PCRE2 regex used to find an event boundary.
	// Mirrors props.conf 'EVENT_BREAKER'.  Default: "([\r\n]+)".
	// The dispatcher (plan §6.0) takes the pure-Go bytes.IndexByte fast path
	// for a strict allow-list of equivalent default patterns; everything else
	// goes through PCRE2 (or RE2 on !cgo builds).
	EventBreaker string `mapstructure:"event_breaker"`

	// LBChunkBreaker is the deprecated httpout-mode breaker.  Only honored
	// when Config.HTTPOutCompat is true.  Mirrors props.conf 'LB_CHUNK_BREAKER'.
	LBChunkBreaker string `mapstructure:"lb_chunk_breaker"`

	// LBChunkBreakerTruncate is the byte cap for httpout chunks.  Only honored
	// when Config.HTTPOutCompat is true.  Mirrors 'LB_CHUNK_BREAKER_TRUNCATE'.
	LBChunkBreakerTruncate int `mapstructure:"lb_chunk_breaker_truncate"`
}

// RegexLimits bundles PCRE2 hardening knobs.  See plan §8.
type RegexLimits struct {
	// MatchLimit is passed to pcre2_set_match_limit; bounds the number of
	// match operations PCRE2 can perform before declaring failure.  Defends
	// against ReDoS even in the cgo path.
	MatchLimit uint32 `mapstructure:"match_limit"`

	// DepthLimit is passed to pcre2_set_depth_limit; bounds the recursion
	// depth PCRE2 can use during backtracking.
	DepthLimit uint32 `mapstructure:"depth_limit"`

	// TimeoutMS is the wall-clock guard applied via a watchdog around each
	// match call.  0 disables the timeout (relying on MatchLimit/DepthLimit only).
	TimeoutMS int `mapstructure:"timeout_ms"`

	// MaxPatternBytes rejects oversize EVENT_BREAKER patterns at config-load.
	MaxPatternBytes int `mapstructure:"max_pattern_bytes"`
}

// sourcetypeKeyAllowed validates the charset of map keys to prevent map-bombing
// with attacker-controlled sourcetype values flowing in from upstream pipelines.
// Plan §8: cap on cached rules + charset allow-list.
var sourcetypeKeyAllowed = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,256}$`)

// Allowed values for Config.DoneKeyEmission. Mirrors design doc §9.3.
const (
	DoneKeyEmissionAlways = "always"
	DoneKeyEmissionAuto   = "auto"
	DoneKeyEmissionNever  = "never"
)

// Hard upper bound on LBChunkBreakerTruncate to refuse outsized values.
// Splunk's default is 2 MiB; we allow up to 100 MiB before refusing.
const maxLBTruncateAbsolute = 100 * 1024 * 1024

// Validate performs config-load-time checks.  This runs before any LogRecord
// reaches the processor, so it is the right place for expensive (one-shot)
// pattern compilation dry-runs in a future revision.
func (c *Config) Validate() error {
	if c.SourcetypeAttribute == "" {
		return errors.New("chunkedlb: sourcetype_attribute must not be empty")
	}
	// done_key_attribute is now optional: empty means "do not emit the
	// legacy boolean attribute on the marker" (the canonical splunkctl
	// payload is always emitted regardless).
	if c.Regex.MaxPatternBytes <= 0 {
		return errors.New("chunkedlb: regex.max_pattern_bytes must be positive")
	}
	if c.Regex.MatchLimit == 0 {
		return errors.New("chunkedlb: regex.match_limit must be positive")
	}
	if c.Regex.DepthLimit == 0 {
		return errors.New("chunkedlb: regex.depth_limit must be positive")
	}
	if c.Regex.TimeoutMS < 0 {
		return errors.New("chunkedlb: regex.timeout_ms must be non-negative")
	}
	if c.MaxCachedRules <= 0 {
		return errors.New("chunkedlb: max_cached_rules must be positive")
	}
	switch c.DoneKeyEmission {
	case DoneKeyEmissionAlways, DoneKeyEmissionAuto, DoneKeyEmissionNever:
		// ok
	case "":
		return errors.New("chunkedlb: done_key_emission must not be empty (allowed: always|auto|never)")
	default:
		return fmt.Errorf("chunkedlb: invalid done_key_emission %q (allowed: always|auto|never)",
			sanitizeForError(c.DoneKeyEmission))
	}

	if err := validateRule("default_rule", &c.DefaultRule, &c.Regex, c.HTTPOutCompat); err != nil {
		return err
	}

	for st, rule := range c.Rules {
		if !sourcetypeKeyAllowed.MatchString(st) {
			return fmt.Errorf("chunkedlb: sourcetype %q does not match allowed charset", sanitizeForError(st))
		}
		ruleCopy := rule
		if err := validateRule("rules."+st, &ruleCopy, &c.Regex, c.HTTPOutCompat); err != nil {
			return err
		}
	}
	return nil
}

func validateRule(label string, r *RuleConfig, limits *RegexLimits, httpout bool) error {
	if r.EventBreaker != "" && len(r.EventBreaker) > limits.MaxPatternBytes {
		return fmt.Errorf("chunkedlb: %s.event_breaker exceeds max_pattern_bytes (%d)", label, limits.MaxPatternBytes)
	}
	if r.LBChunkBreaker != "" && len(r.LBChunkBreaker) > limits.MaxPatternBytes {
		return fmt.Errorf("chunkedlb: %s.lb_chunk_breaker exceeds max_pattern_bytes (%d)", label, limits.MaxPatternBytes)
	}
	if r.LBChunkBreakerTruncate < 0 {
		return fmt.Errorf("chunkedlb: %s.lb_chunk_breaker_truncate must be non-negative", label)
	}
	if r.LBChunkBreakerTruncate > maxLBTruncateAbsolute {
		return fmt.Errorf("chunkedlb: %s.lb_chunk_breaker_truncate exceeds absolute cap (%d bytes)", label, maxLBTruncateAbsolute)
	}
	if !httpout && (r.LBChunkBreaker != "" || r.LBChunkBreakerTruncate != 0) {
		// Mirrors LB_CHUNK_BREAKER's "Only used if [httpout] is configured" semantics.
		// Surfacing as a hard error so misconfigurations don't silently get ignored.
		return fmt.Errorf("chunkedlb: %s sets lb_chunk_breaker* but httpout_compat is false", label)
	}
	return nil
}

// sanitizeForError strips CR/LF from any user-provided string before logging
// (codeguard-0-logging: prevent log-injection via attacker-supplied keys).
func sanitizeForError(s string) string {
	if len(s) > 64 {
		s = s[:64]
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\r' || c == '\n' || c < 0x20 {
			out = append(out, '?')
			continue
		}
		out = append(out, c)
	}
	return string(out)
}

// timeoutDuration returns the configured per-match wall-clock timeout, or 0
// to indicate "no wall-clock timeout".
func (c *Config) timeoutDuration() time.Duration {
	if c.Regex.TimeoutMS <= 0 {
		return 0
	}
	return time.Duration(c.Regex.TimeoutMS) * time.Millisecond
}
