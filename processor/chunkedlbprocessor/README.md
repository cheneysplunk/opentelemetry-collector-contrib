# chunkedlb processor

Go port of Splunk Enterprise 10.4's `ChunkedLBProcessor`
(`develop/splunk-10.4/src/input/ChunkedLBProcessor.cpp`). Splits each
inbound `LogRecord` body at the configured event-breaker into
`[firstHalf, marker, tail]` triples so the downstream
`AutoLoadBalancedConnectionStrategy` (in the C++ pipeline) — or its
OTel analog, the `splunks2s` exporter's `sticky_by_channel` LB — can
route at chunk boundaries without fusing events on the indexer's
per-channel `LineBreakingProcessor` buffer.

See `processor.go` for design notes and `pdataemit/pdataemit.go` for
the contiguous-ordering invariant that makes this work in OTel's
shared `ScopeLogs` slice.

## Operating modes

### Streaming mode (default; `treat_input_as_complete_events: false`)

Mirrors C++ `ChunkedLBProcessor::executeMulti`. Every input
`LogRecord` whose body contains an event-breaker is split into
`[firstHalf, marker, tail]`. The marker carries the canonical
`splunkctl.FlagDone` payload; downstream Splunk-aware exporters
translate it to a wire-level `pd.SetDone()`.

Use this mode when the upstream is a streaming receiver
(`splunk_tcpinput`, journald, `tcplog`, ...) that emits raw byte
chunks without per-record event boundaries.

### Pre-broken mode (`treat_input_as_complete_events: true`)

Stamps `splunkctl.FlagDone` on every non-empty input record without
splitting. Mirrors C++ `ChunkedLBProcessor::shouldProcess`'s
`hasDoneKey()` pass-through path. Use this mode when the upstream is
a receiver that already line-breaks records (`filelog` with
multiline, `syslog` parser).

## Multi-receiver semantics

`chunkedlb` is **stateless per record across receivers** — the
classification and split logic looks only at the input record's body
and attributes. Two `splunk_tcpinput` receivers (or any mix of
streaming sources) can fan into one `chunkedlb` instance with no
cross-talk.

Three invariants make this safe:

1. **No per-channel buffer.** The processor does not accumulate bytes
   across records. Each `processLogs` call walks the input
   `ScopeLogs` exactly once and emits the rewritten records
   immediately.

2. **Resource attributes pass through.** The split path operates on
   `ScopeLogs.LogRecords()` and never touches `Resource`. Receivers
   like `splunk_tcpinput` that stamp a per-connection
   `_splunk_channel` resource attribute can rely on every output
   record (firstHalf, marker, tail) inheriting it. The
   `processor_channel_preservation_test.go` test pins this invariant
   so it cannot regress silently.

3. **Per-instance configuration.** Each `chunkedlb` instance carries
   its own `treat_input_as_complete_events` setting. A pipeline
   needing both streaming and pre-broken inputs should run two
   `chunkedlb` instances in parallel pipelines, each tuned for its
   upstream.

## Sticky-LB requirement

When the downstream is `splunks2s` with `loadBalancing:
sticky_by_channel`, every receiver feeding `chunkedlb` MUST stamp
`_splunk_channel` (the receiver-issued sticky-routing key). Without
it, the exporter falls back to RoundRobin and the three records of
each split can land on different indexers — the indexer's
per-channel `LineBreakingProcessor` then fuses tail of split N with
firstHalf of split N+1 because they ride the same wire channel.

This is exactly the bug the
`receiver/splunktcpinputreceiver/README.md` "Stickiness" section
describes; the receiver and exporter are designed as a pair to
solve it end-to-end.

## Phase 2 roadmap

* **`SplunkLineBreakingProcessor`** — full Go port of the indexer
  `LineBreakingProcessor` so the OTel pipeline can do final
  per-event line-breaking instead of delegating it to a downstream
  Splunk indexer.
* **`AggregatorProcessor`** — `pd.SetFlush()` emission for the
  AutoLB hand-off path; reserved bit `splunkctl.FlagFlush` already
  exists for it.
* **Per-channel metrics** — splits/sec, marker emissions,
  pass-through rate, by sourcetype.
