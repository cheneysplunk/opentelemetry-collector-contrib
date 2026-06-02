# splunkexecreceiver

OpenTelemetry Collector receiver that runs native Splunk scripted inputs through
`splunkframeworkextension` and forwards their output as OTel logs.

The receiver is pure Go. CGo and Splunk headers remain inside
`splunkframeworkextension`, which calls `framework_cabi/libsplunk_cabi.so`.

## Configuration

```yaml
extensions:
  splunkframework:
    splunk_home: /opt/splunk

receivers:
  splunkexec:
    framework: splunkframework
    scripts:
      - command: ./bin/my_input
        interval: "60"
        sourcetype: my_input
        index: main
        start_by_shell: false
    inputs_conf: |
      [my_modular_input]
      run_introspection = true

      [my_modular_input://example]
      disabled = 0
      interval = 60
      sourcetype = my_modular_input

service:
  extensions: [splunkframework]
  pipelines:
    logs:
      receivers: [splunkexec]
      exporters: [...]
```

Each script entry is rendered into an in-memory native stanza:

```ini
[script://./bin/my_input]
disabled = 0
interval = 60
sourcetype = my_input
index = main
start_by_shell = false
```

`inputs_conf` is optional raw native `inputs.conf` text. Use it for modular
inputs or other exec inputs that need full stanza control. The app containing
the modular input executable and `README/inputs.conf.spec` must still be present
under `$SPLUNK_HOME/etc/apps`.

`ExecProcessor` keeps Splunk's scripted-input validation. Commands must resolve
to valid native scripted input locations, such as `$SPLUNK_HOME/bin/scripts` or
an app/bundle `bin` directory.

## Supported input types

This receiver supports both native Splunk exec input styles through
`splunkframeworkextension`:

| Input type | Receiver config | Native stanzas |
|---|---|---|
| Scripted input | `scripts` | `[script://...]` |
| Modular input | `inputs_conf` | `[scheme]` and `[scheme://name]` |

Scripted inputs are convenient to configure with the structured `scripts` list.
Modular inputs should use `inputs_conf` because the native scheme stanza and
modular input instance stanza often both matter.

## Modular input XML protocol

The exec receiver uses Splunk's modular input XML protocol. The native
`ExecProcessor` generates an `<input>` XML document and pipes it to the
script's stdin. The script writes `<event>` elements wrapped in a `<stream>`
to stdout, which the receiver parses into OTel log records.

**Input XML (piped to the script's stdin):**

```xml
<input>
  <server_host>otel-exec-test-host</server_host>
  <server_uri>https://localhost:8089</server_uri>
  <session_key>test-session-key-123</session_key>
  <checkpoint_dir>/tmp/otel_exec_checkpoint</checkpoint_dir>
  <configuration>
    <stanza name="otel_exec_python_file_input://exec_receiver">
      <param name="file_path">/tmp/otel_exec_test_input.txt</param>
      <param name="sourcetype">otel_exec_python_file_input</param>
      <param name="source">otel_exec_python_file_input_source</param>
      <param name="host">otel-python-file-host</param>
      <param name="index">main</param>
    </stanza>
  </configuration>
</input>
```

**Output XML (written to stdout by the script):**

```xml
<stream>
  <event unbroken="1" stanza="otel_exec_python_file_input://exec_receiver">
    <source>otel_exec_python_file_input_source</source>
    <sourcetype>otel_exec_python_file_input</sourcetype>
    <index>main</index>
    <host>otel-python-file-host</host>
    <time>1780443803.721</time>
    <data>hello from exec receiver</data>
    <done/>
  </event>
</stream>
```

Each `<event>` is parsed into an OTel log record with source/sourcetype/host
mapped to log attributes and `<data>` as the log body.

## Runtime ownership

Use exactly one shared `splunkframeworkextension` for Splunk-backed OTel
components in a Collector process. The extension owns the native Splunk C++
runtime: `SplunkMainThread`, `EventLoop`, `PropertyPages`, plugin factories,
`ProcessRunner`, and `libsplunk_cabi.so`.

The receiver stays pure Go and obtains the extension by component ID:

```text
splunkframeworkextension
  owns native Splunk runtime and CGo boundary

splunkexecreceiver
  renders inputs.conf text and calls fw.NewPipeline(InputsConf: ...)

splunktailreceiver / splunktcpoutexporter
  use the same extension for tail and S2S output paths
```

Do not create separate native Splunk runtime extensions for exec, tail, and
tcpout. Those would conflict on process-global native state and lifecycle
ownership. Keep the runtime merged in `splunkframeworkextension`, while keeping
receivers and exporters separate by OTel signal/component responsibility.

## Scalability model

Multiple exec inputs do not create one pipeline set per input. The native model
matches Splunk's `ExecProcessor`: configured inputs become scheduled
`ExecedCommand` objects in shared exec processor state, and runnable commands
are dispatched through the exec processor run queue.

With one `splunkexecreceiver` configured with 100 inputs, the expected shape is:

```text
1 splunkframeworkextension
1 active exec processor pipeline session
1 exec pipeline set today
100 ExecedCommand objects
up to 100 child processes only if 100 inputs are runnable or long-lived at once
```

Those inputs run as separate subprocesses when scheduled, but they share the
same exec pipeline resources:

```text
100 configured inputs
  -> shared ExecProcessor scheduler/run queue
  -> child subprocesses launched as needed
  -> stdout read from each running child
  -> shared native pipeline input queue
  -> shared parsing/event handling path
  -> one Go callback/event channel
  -> splunkexecreceiver forwards OTel logs downstream
```

The main scaling pressure is therefore not pipeline count. It is external
process concurrency, process churn for short intervals, bytes emitted by child
processes, parsing cost, native queue memory, Go callback/channel throughput,
and downstream exporter backpressure.

ExecProcessor intentionally throttles startup through a run queue so many
configured scripts are not launched all at once. A command is not re-entered
while it is already running.

## Performance considerations

Today's preferred production shape is one `splunkexecreceiver` instance with all
exec inputs configured in that receiver. This keeps one native exec pipeline and
avoids multiplying Splunk runtime state.

If many inputs are active at the same time, they can still compete for shared
pipeline resources. For example, 100 long-running modular inputs can mean 100
child processes emitting data in parallel into one shared native pipeline and
one shared Go event channel. That can be healthy if each input is light, but it
can become a bottleneck if inputs are CPU-heavy, emit large bursts, or run at
very short intervals.

Useful tuning and design levers are:

| Lever | Effect |
|---|---|
| Increase input intervals | reduces process churn and burst frequency |
| Avoid long-running noisy inputs when possible | reduces concurrent stdout pressure |
| Keep one receiver but many inputs | preserves single native runtime ownership |
| Use downstream batch/queue/exporter tuning | absorbs bursts after events become OTel logs |
| Add per-input or global concurrency limits in future | caps active child process count |

Potential future scaling work could expose a bounded exec pipeline count, for
example `pipeline_count: 2`, `4`, or `8`. The receiver would shard input stanzas
across those native exec pipelines instead of creating one pipeline per input.
That would trade extra native pipeline memory and scheduling overhead for lower
contention in each pipeline.

A future multi-pipeline design could also reuse a receiver-side load-balancing
or sharding layer:

```text
splunkexecreceiver
  -> split rendered inputs.conf into N groups
  -> fw.NewPipeline(InputsConf: group 0)
  -> fw.NewPipeline(InputsConf: group 1)
  -> ...
  -> fan in Pipeline.Events() from all groups
  -> forward one OTel logs stream downstream
```

That design should stay bounded and explicit. Prefer `N` pipelines as worker
capacity, not `N` pipelines for `N` inputs. It would also require relaxing the
current native C ABI active-handle guard and adding clear receiver-level
lifecycle ownership for multiple exec pipelines.

## Single receiver constraint

Configure exec inputs in one `splunkexecreceiver` instance per Collector
process. Put multiple scripted and modular inputs into that one receiver.

The native C ABI currently enforces this with a process-global active-handle
guard: a second active exec pipeline fails during startup. A clearer
extension-level guard, similar to the tail receiver's monitor-pipeline claim,
is a good future refinement so duplicate receiver configuration fails with an
explicit OTel-facing message.

## Output

The receiver forwards raw bytes from the native exec pipeline into
`plog.LogRecord.Body` as bytes and preserves native metadata:

| Metadata | OTel location |
|---|---|
| host | resource attribute `host.name` |
| source | log attribute `splunk.source` |
| sourcetype | log attribute `splunk.sourcetype` |

## Build

Include both `splunkframeworkextension` and this receiver in the Collector
Builder config. Only the extension needs CGo linker flags for
`libsplunk_cabi.so`.

## How it works

At runtime the receiver has two jobs:

1. Convert OTel receiver config into native Splunk `inputs.conf` stanza text.
2. Ask `splunkframeworkextension` to start a native exec pipeline with that
   stanza text.

The startup flow is:

```text
Collector config
  -> splunkexecreceiver
  -> render inputs.conf text
  -> splunkframeworkextension.NewPipeline(InputsConf: ...)
  -> framework_cabi/libsplunk_cabi.so
  -> native ExecProcessor
  -> child input process
  -> stdout XML/raw events
  -> Go callback
  -> OTel plog.Logs
```

For structured `scripts` config, `splunkexecreceiver` renders
`[script://...]` stanzas itself. For modular inputs, pass raw native
`inputs_conf` text so the scheme stanza, instance stanza, `python.required`,
metadata, and app-specific parameters are preserved exactly.

For Python modular inputs, the native `ExecProcessor` discovers the executable
from the installed app under `$SPLUNK_HOME/etc/apps/<app>/bin`. When the command
is a `.py` file, Splunk prepends the selected Splunk Python interpreter. In the
test fixture that is:

```ini
[otel_exec_python_file_input]
python.required = 3.9
```

So the launched process looks like:

```text
/home/chli/splunk_home/bin/python3.9 \
  /home/chli/splunk_home/etc/apps/otel_exec_python_file_app/bin/otel_exec_python_file_input.py
```

The fixture app's input reads Splunk modular input XML from stdin, reads the
configured file path, emits one XML `<event>` per line, and then calls the
configured REST root to create, read, update, and delete a conf stanza.

## Tests

The integration test `TestSplunkExecReceiverRunsFixtureAppInputsConf` starts
`splunkframeworkextension` once and runs a real fixture app from
`testdata/python_file_modinput_app`.

The fixture app contains:

| Path | Purpose |
|---|---|
| `default/app.conf` | minimal Splunk app metadata |
| `default/inputs.conf` | real modular input config used by the test |
| `README/inputs.conf.spec` | modular input spec |
| `bin/otel_exec_python_file_input.py` | Python modular input implementation |

The test uses a generic fixture-app helper. It copies the app into
`$SPLUNK_HOME/etc/apps`, renders placeholders in the fixture's
`default/inputs.conf`, and feeds that rendered text to the receiver's
`inputs_conf` setting. This matches the receiver's current startup path:
`splunkexecreceiver` explicitly calls `fw.NewPipeline(InputsConf: ...)`; it does
not yet auto-scan installed app `default/inputs.conf` files by itself.

To reuse the test harness for another input, add another app under `testdata`,
include a real `default/inputs.conf`, and call `installFixtureApp` with the
fixture directory, installed app name, and any runtime placeholder values.

The Python fixture verifies:

| Feature | Verification |
|---|---|
| input scheduler | `interval = -1` runs the modular input once |
| direct `.py` launch | native `ExecProcessor` prepends Splunk Python selected by `python.required = 3.9` |
| stdin/stdout protocol | the input parses Splunk modular input XML and emits XML events |
| file ingestion | each line from the configured file becomes a log record |
| REST config CRUD | the input calls `/services/auth/login` and `/servicesNS/nobody/system/configs/conf-*` |

The same `rest_url` field can point at the `splunkframeworkextension`
management port when native REST is enabled.

### Run the integration test

From the receiver directory:

```bash
cd /home/chli/otel/opentelemetry-collector-contrib/receiver/splunkexecreceiver
```

Check the Python modular input syntax:

```bash
python3 -m py_compile \
  testdata/python_file_modinput_app/bin/otel_exec_python_file_input.py
rm -rf testdata/python_file_modinput_app/bin/__pycache__
```

Run only this receiver's tests through the real Splunk framework C ABI:

```bash
CGO_LDFLAGS='-L/home/chli/main/src/framework_cabi -Wl,-rpath,/home/chli/main/src/framework_cabi -Wl,-rpath,/home/chli/splunk_home/lib -lsplunk_cabi -lstdc++ -ldl -lpthread' \
LD_LIBRARY_PATH='/home/chli/main/src/framework_cabi:/home/chli/splunk_home/lib' \
SPLUNK_HOME=/home/chli/splunk_home \
go test ./... -count=1 -v
```

Run just the fixture app integration test:

```bash
CGO_LDFLAGS='-L/home/chli/main/src/framework_cabi -Wl,-rpath,/home/chli/main/src/framework_cabi -Wl,-rpath,/home/chli/splunk_home/lib -lsplunk_cabi -lstdc++ -ldl -lpthread' \
LD_LIBRARY_PATH='/home/chli/main/src/framework_cabi:/home/chli/splunk_home/lib' \
SPLUNK_HOME=/home/chli/splunk_home \
go test . -run TestSplunkExecReceiverRunsFixtureAppInputsConf -count=1 -v
```

A successful run should include:

```text
New scheduled exec process: /home/chli/splunk_home/bin/python3.9 ...
--- PASS: TestSplunkExecReceiverRunsFixtureAppInputsConf
PASS
```

The test asserts all of these behaviors:

| Assertion | What it proves |
|---|---|
| file marker log is received | scheduler launched the input and file lines became OTel logs |
| `splunk.source`, `splunk.sourcetype`, `host.name` match | native metadata survived receiver conversion |
| REST status log contains `conf=otel_exec_python_file_test` | the input completed REST CRUD |
| REST fake server saw login, create, read, update, delete | conf management calls used the expected endpoints |
| status log contains `python3.9` | `.py` was launched through Splunk Python from `python.required` |

To inspect the modular input scheme manually:

```bash
/home/chli/splunk_home/bin/python3.9 \
  testdata/python_file_modinput_app/bin/otel_exec_python_file_input.py \
  --scheme
```
