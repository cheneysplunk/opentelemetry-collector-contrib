// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package metadata

import "go.opentelemetry.io/collector/component"

// Type is the unique component type identifier for the chunkedlb processor.
// Mirrors the Splunk C++ ChunkedLBProcessor (develop/splunk-10.4/src/input/ChunkedLBProcessor.cpp).
var Type = component.MustNewType("chunkedlb")

// LogsStability indicates that this processor is in alpha-quality state.
// Bumped to Beta after benchmarks and conformance vs the C++ reference.
const LogsStability = component.StabilityLevelAlpha
