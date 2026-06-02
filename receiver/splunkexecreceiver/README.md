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

The main scaling pressure is therefore not pipeline set count. It is external
process concurrency, process churn for short intervals, bytes emitted by child
processes, parsing cost, queue memory, and downstream exporter throughput.

ExecProcessor intentionally throttles startup through a run queue so many
configured scripts are not launched all at once. A command is not re-entered
while it is already running. If future work exposes multiple exec pipeline sets,
treat that as bounded worker capacity, for example `2`, `4`, or `8`, not one
pipeline set per input.

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

## Tests

The integration test `TestSplunkExecReceiverInputsThroughFrameworkExtension`
starts `splunkframeworkextension` once and verifies both input types through the
real extension path:

| Subtest | Fixture |
|---|---|
| `scripted input` | temporary `$SPLUNK_HOME/bin/scripts/otel_exec_scripted_input_*.sh` |
| `modular input` | temporary app under `$SPLUNK_HOME/etc/apps/otel_exec_modinput_app_*` |

Both tests assert that native exec output is delivered as OTel logs with the
expected `host.name`, `splunk.source`, and `splunk.sourcetype` metadata.
