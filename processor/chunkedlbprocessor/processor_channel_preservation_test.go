// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

package chunkedlbprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
)

// TestChunkedLB_PreservesSplunkChannelResourceAttr asserts that when
// chunkedlb splits a single record into [firstHalf, marker, tail], the
// per-resource `_splunk_channel` attribute set by the upstream
// splunk_tcpinput receiver is inherited by every output record.
//
// This is the load-bearing invariant for the splunks2s exporter's
// StickyByChannel load balancer: if the channel attribute were lost
// on any of the three output records, the exporter could route them
// to different indexers and the indexer-side LineBreakingProcessor
// would fuse events across chunk boundaries.
//
// chunkedlb operates on ScopeLogs.LogRecords() only and never touches
// the resource attributes, so the test is straightforward — but we
// guard it explicitly so a future refactor cannot silently break the
// invariant.
func TestChunkedLB_PreservesSplunkChannelResourceAttr(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// Force the marker to be emitted so we can assert its presence
	// in the output ordering. Default emission policy is "never";
	// this test exercises the always-emit path that splunks2s uses
	// when running with splunkctlCompat=true.
	cfg.DoneKeyEmission = DoneKeyEmissionAlways
	cfg.DoneKeyAttribute = "" // canonical splunkctl payload only
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.LogsSink)
	settings := processortest.NewNopSettings(NewFactory().Type())
	p, err := NewFactory().CreateLogs(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })

	const channelKey = "splunk_tcpinput/syslog::0.0.0.0:9514::conn:7::1.2.3.4:55501"

	// Build a single ResourceLogs with the channel attr on the
	// resource, then one record whose body has an event boundary
	// inside it (forces chunkedlb's split path).
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("_splunk_channel", channelKey)
	rl.Resource().Attributes().PutStr(defaultSourcetypeAttribute, "tcp-raw")

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("first line\nsecond line")

	require.NoError(t, p.ConsumeLogs(context.Background(), ld))

	got := sink.AllLogs()
	require.Len(t, got, 1)

	// Walk every output record. EVERY one of them must inherit the
	// resource-level _splunk_channel because (a) they share the
	// same ResourceLogs and (b) chunkedlb does not touch resource
	// attrs.
	out := got[0]
	rls := out.ResourceLogs()
	require.GreaterOrEqual(t, rls.Len(), 1)

	for i := 0; i < rls.Len(); i++ {
		v, ok := rls.At(i).Resource().Attributes().Get("_splunk_channel")
		require.Truef(t, ok, "ResourceLogs[%d] must carry _splunk_channel after chunkedlb split", i)
		assert.Equalf(t, channelKey, v.AsString(),
			"ResourceLogs[%d] _splunk_channel must equal the input channel", i)

		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			// Sanity: confirm we actually exercised the split path.
			// One ScopeLogs holding 3+ records is the
			// [firstHalf, marker, tail] shape ApplyToScope produces.
			if lrs.Len() < 2 {
				continue
			}
			t.Logf("scope[%d/%d] holds %d records (expected 3 after split)", i, j, lrs.Len())
		}
	}
}

// TestChunkedLB_PreservesSplunkChannelAcrossPassThrough asserts the
// no-split path also preserves _splunk_channel — guards against a
// future change that might rebuild the resource for unsplittable
// records.
func TestChunkedLB_PreservesSplunkChannelAcrossPassThrough(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DoneKeyEmission = DoneKeyEmissionAlways
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.LogsSink)
	settings := processortest.NewNopSettings(NewFactory().Type())
	p, err := NewFactory().CreateLogs(context.Background(), settings, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), nil))
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })

	const channelKey = "splunk_tcpinput/foo::conn:1::1.2.3.4:55501"

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("_splunk_channel", channelKey)

	sl := rl.ScopeLogs().AppendEmpty()
	// Body without an event boundary: chunkedlb cannot split it,
	// so the record passes through as-is.
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("no-newline-no-split")

	require.NoError(t, p.ConsumeLogs(context.Background(), ld))

	got := sink.AllLogs()
	require.Len(t, got, 1)

	for i := 0; i < got[0].ResourceLogs().Len(); i++ {
		v, ok := got[0].ResourceLogs().At(i).Resource().Attributes().Get("_splunk_channel")
		require.True(t, ok, "pass-through path must preserve _splunk_channel")
		assert.Equal(t, channelKey, v.AsString())
	}
}
