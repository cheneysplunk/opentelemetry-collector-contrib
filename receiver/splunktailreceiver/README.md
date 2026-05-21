# splunktailreceiver

An OpenTelemetry Collector receiver that forwards native Splunk file-tail input
data into an OTel logs pipeline.

The receiver no longer owns Splunk CGo or receiver-side monitor config. It
looks up `splunkframeworkextension`, asks the extension to create a native
TailManager pipeline from the extension-owned merged `inputs.conf` cache, and
then forwards the raw byte chunks delivered by that pipeline.

**Output body type:** `ValueTypeBytes` when the CABI delivers raw bytes. The
receiver preserves those bytes in `plog.LogRecord.Body`.

## Pipeline

```
$SPLUNK_HOME/etc/**/inputs.conf
      │
      │  loaded/merged by splunkframeworkextension at startup
      ▼
splunkframeworkextension             [CGo lives here]
  ConfManager / PropertyPages cache
  NewMonitorPipeline()
      │
      ▼
libsplunk_cabi.so
  TailManager / TailWatcher / TailReader
  WatchedTailFile::readChunk()
      │
      │ raw bytes + native source/sourcetype/host metadata
      ▼
splunktailreceiver                   [pure Go]
  plog.LogRecord.Body: ValueTypeBytes
  attributes: splunk.source, splunk.sourcetype
      │
      ▼
OTel processors / exporters
```

The receiver does **not** create `monitor://` stanzas from OTel config. To add,
change, or remove tailed files, edit `inputs.conf` or use the extension's native
`configs/conf-inputs` REST API.

## Configuration

```yaml
extensions:
  splunkframework:
    splunk_home: /opt/splunk
    management_port: 8089   # optional; enables auth/login + configs/conf-* REST

receivers:
  splunktail:
    framework: splunkframework

service:
  extensions: [splunkframework]
  pipelines:
    logs:
      receivers: [splunktail]
      exporters: [...]
```

Example `inputs.conf`:

```ini
[monitor:///var/log/myapp/*.log]
disabled = 0
sourcetype = myapp
index = main
```

Legacy receiver fields such as `monitors`, `splunk_home`,
`default_sourcetype`, `default_index`, `host`, and `fishbucket_dir` are accepted
for config compatibility but ignored. The extension owns `SPLUNK_HOME`, and the
native TailManager owns monitor behavior.

## Behavior

- Startup matches the Splunk shape more closely: the extension initializes the
  framework and merged conf cache first; the receiver starts a tail pipeline
  from that cache.
- Only one `splunktailreceiver` may be active per Collector process. The native
  tail CABI has one process-global callback path and parsing queue, so the
  extension rejects a second monitor pipeline instead of risking duplicated or
  misrouted data.
- Full monitor stanza behavior stays native because TailManager reads
  `inputs.conf` directly. Settings such as `blacklist`, `whitelist`,
  `recursive`, `crcSalt`, `ignoreOlderThan`, and fishbucket state are not
  re-modeled in Go.
- `configs/conf-*` REST CRUD changes do not automatically restart the running
  tail pipeline. Use explicit reload/restart behavior when changing active
  inputs, matching the current conf-management design decision.

## Building

Build the unified collector with `splunkframeworkextension` included. Only the
extension needs linker flags for `libsplunk_cabi.so`.

```bash
DEMO=/home/chli/otel/opentelemetry-collector-contrib/exporter/splunktcpoutexporter/demo
FW_DIR=/home/chli/main/src/framework_cabi
SPLUNK_HOME=/home/chli/splunk_home

CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi \
             -Wl,-rpath,${FW_DIR} \
             -Wl,-rpath,${SPLUNK_HOME}/lib \
             -lstdc++ -ldl -lpthread" \
GOPATH=/home/chli/go \
/home/chli/go/bin/builder --config ${DEMO}/builder-config-unified.yaml
```

## Running

```bash
LD_LIBRARY_PATH=/home/chli/main/src/framework_cabi:/home/chli/splunk_home/lib \
SPLUNK_HOME=/home/chli/splunk_home \
${DEMO}/bin-unified/splunk-col --config ${DEMO}/config-unified.yaml
```

## Key Source Files

| File | Purpose |
|---|---|
| `receiver.go` | Looks up `splunkframeworkextension`, starts `NewMonitorPipeline`, forwards events |
| `config.go` | Receiver config; only `framework` is active |
| `extension/splunkframeworkextension/monitor_pipeline.go` | Creates native monitor pipeline from merged `inputs.conf` |
| `main/src/input/tail_lib/tailin_cabi.cpp` | Native TailManager CABI; can start from existing `inputs.conf` without programmatic monitors |
