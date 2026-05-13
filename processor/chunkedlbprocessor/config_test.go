// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validBaseConfig() *Config {
	return &Config{
		SourcetypeAttribute: defaultSourcetypeAttribute,
		DoneKeyAttribute:    defaultDoneKeyAttribute,
		DoneKeyEmission:     DoneKeyEmissionAlways,
		DefaultRule: RuleConfig{
			EventBreakerEnable: false,
			EventBreaker:       defaultEventBreaker,
		},
		Rules:          map[string]RuleConfig{},
		MaxCachedRules: defaultMaxCachedRules,
		Regex: RegexLimits{
			MatchLimit:      defaultMatchLimit,
			DepthLimit:      defaultDepthLimit,
			TimeoutMS:       defaultTimeoutMS,
			MaxPatternBytes: defaultMaxPatternBytes,
		},
	}
}

func TestConfig_ValidDefault(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())
}

func TestConfig_RequiresSourcetypeAttribute(t *testing.T) {
	cfg := validBaseConfig()
	cfg.SourcetypeAttribute = ""
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "sourcetype_attribute")
}

func TestConfig_DoneKeyAttributeMayBeEmpty(t *testing.T) {
	// As of the splunkctl migration (design doc §3.10), the canonical
	// marker payload is the splunkctl bytes attribute. The legacy
	// boolean DoneKeyAttribute is now optional: empty means "skip the
	// legacy attribute entirely; emit splunkctl only". Validate() must
	// accept that.
	cfg := validBaseConfig()
	cfg.DoneKeyAttribute = ""
	require.NoError(t, cfg.Validate(),
		"empty done_key_attribute is now valid (splunkctl-only marker)")
}

func TestConfig_RegexLimitsMustBePositive(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mut   func(c *Config)
		want  string
	}{
		{"max_pattern_bytes", func(c *Config) { c.Regex.MaxPatternBytes = 0 }, "max_pattern_bytes"},
		{"match_limit", func(c *Config) { c.Regex.MatchLimit = 0 }, "match_limit"},
		{"depth_limit", func(c *Config) { c.Regex.DepthLimit = 0 }, "depth_limit"},
		{"max_cached_rules", func(c *Config) { c.MaxCachedRules = 0 }, "max_cached_rules"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validBaseConfig()
			tc.mut(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestConfig_RejectsOversizePattern(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Regex.MaxPatternBytes = 16
	cfg.Rules = map[string]RuleConfig{
		"my_st": {
			EventBreakerEnable: true,
			EventBreaker:       strings.Repeat("a", 17),
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max_pattern_bytes")
}

func TestConfig_RejectsBadSourcetypeKey(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Rules = map[string]RuleConfig{
		"bad/sourcetype with space": {
			EventBreakerEnable: false,
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "allowed charset")
}

func TestConfig_RejectsLBChunkBreakerWithoutHTTPOut(t *testing.T) {
	cfg := validBaseConfig()
	cfg.HTTPOutCompat = false
	cfg.Rules = map[string]RuleConfig{
		"my_st": {
			EventBreakerEnable: true,
			EventBreaker:       defaultEventBreaker,
			LBChunkBreaker:     defaultLBChunkBreaker,
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "httpout_compat")
}

func TestConfig_AcceptsLBChunkBreakerWithHTTPOut(t *testing.T) {
	cfg := validBaseConfig()
	cfg.HTTPOutCompat = true
	cfg.Rules = map[string]RuleConfig{
		"my_st": {
			EventBreakerEnable:     true,
			EventBreaker:           defaultEventBreaker,
			LBChunkBreaker:         defaultLBChunkBreaker,
			LBChunkBreakerTruncate: 1024,
		},
	}
	require.NoError(t, cfg.Validate())
}

func TestConfig_RejectsLBChunkBreakerTruncateAboveCap(t *testing.T) {
	cfg := validBaseConfig()
	cfg.HTTPOutCompat = true
	cfg.Rules = map[string]RuleConfig{
		"my_st": {
			EventBreakerEnable:     true,
			LBChunkBreakerTruncate: maxLBTruncateAbsolute + 1,
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "absolute cap")
}

func TestSanitizeForError_StripsControlChars(t *testing.T) {
	got := sanitizeForError("good\nname\rwith\x01stuff")
	require.NotContains(t, got, "\n")
	require.NotContains(t, got, "\r")
	require.NotContains(t, got, "\x01")
}
func TestConfig_RejectsInvalidDoneKeyEmission(t *testing.T) {
	cfg := validBaseConfig()
	cfg.DoneKeyEmission = "sometimes"
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "done_key_emission")
}

func TestConfig_RejectsEmptyDoneKeyEmission(t *testing.T) {
	cfg := validBaseConfig()
	cfg.DoneKeyEmission = ""
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "done_key_emission")
}

func TestConfig_AcceptsAllDoneKeyEmissionValues(t *testing.T) {
	for _, v := range []string{
		DoneKeyEmissionAlways,
		DoneKeyEmissionAuto,
		DoneKeyEmissionNever,
	} {
		t.Run(v, func(t *testing.T) {
			cfg := validBaseConfig()
			cfg.DoneKeyEmission = v
			require.NoError(t, cfg.Validate())
		})
	}
}


