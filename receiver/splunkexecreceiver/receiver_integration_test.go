// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkexecreceiver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	splunkframeworkextension "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

const testFrameworkType = "splunkframework"

type testHost struct {
	extensions map[component.ID]component.Component
}

func (h testHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

type capturedLog struct {
	body       string
	source     string
	sourcetype string
	host       string
}

type logsSink struct {
	records chan capturedLog
}

func newLogsSink() *logsSink {
	return &logsSink{records: make(chan capturedLog, 64)}
}

func (s *logsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (s *logsSink) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		host := ""
		if value, ok := rl.Resource().Attributes().Get("host.name"); ok {
			host = value.Str()
		}
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				record := capturedLog{host: host}
				if lr.Body().Type() == pcommon.ValueTypeBytes {
					record.body = string(lr.Body().Bytes().AsRaw())
				} else {
					record.body = lr.Body().AsString()
				}
				if value, ok := lr.Attributes().Get("splunk.source"); ok {
					record.source = value.Str()
				}
				if value, ok := lr.Attributes().Get("splunk.sourcetype"); ok {
					record.sourcetype = value.Str()
				}
				select {
				case s.records <- record:
				default:
				}
			}
		}
	}
	return nil
}

func (s *logsSink) waitForMarker(marker string, timeout time.Duration) (capturedLog, error) {
	deadline := time.After(timeout)
	for {
		select {
		case record := <-s.records:
			if strings.Contains(record.body, marker) {
				return record, nil
			}
		case <-deadline:
			return capturedLog{}, fmt.Errorf("timed out waiting for marker %q", marker)
		}
	}
}

func TestSplunkExecReceiverInputsThroughFrameworkExtension(t *testing.T) {
	ctx := context.Background()
	splunkHome, splunkDB := setupSplunkTestEnv(t, "otel_exec_db_")

	scriptedMarker := "otel-exec-scripted-ok"
	scriptPath := writeScriptedInput(t, splunkHome, scriptedMarker)

	modularMarker := "otel-exec-modular-ok"
	scheme := "otel_exec_modinput_" + uniqueSuffix()
	writeModularInputApp(t, splunkHome, scheme, modularMarker)

	frameworkID, framework := startFrameworkExtension(t, ctx, splunkHome, splunkDB)

	t.Run("scripted input", func(t *testing.T) {
		sink := newLogsSink()
		rcv := startExecReceiver(t, ctx, frameworkID, framework, sink, &Config{
			Framework: frameworkID,
			Scripts: []ScriptConfig{{
				Command:      scriptPath,
				Interval:     "-1",
				Sourcetype:   "otel_exec_scripted",
				Index:        "main",
				Host:         "otel-exec-scripted-host",
				Source:       "otel_exec_scripted_source",
				StartByShell: false,
			}},
		})
		t.Cleanup(func() {
			if err := rcv.Shutdown(ctx); err != nil {
				t.Errorf("receiver shutdown failed: %v", err)
			}
		})

		record, err := sink.waitForMarker(scriptedMarker, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		assertCapturedLog(t, record, "otel_exec_scripted_source", "otel_exec_scripted", "otel-exec-scripted-host")
	})

	t.Run("modular input", func(t *testing.T) {
		sink := newLogsSink()
		rcv := startExecReceiver(t, ctx, frameworkID, framework, sink, &Config{
			Framework: frameworkID,
			InputsConf: fmt.Sprintf(`
[%s]
run_introspection = true
run_only_one = false

[%s://example]
disabled = 0
interval = -1
marker = %s
sourcetype = otel_exec_modular
source = otel_exec_modular_source
host = otel-exec-modular-host
index = main
start_by_shell = false
`, scheme, scheme, modularMarker),
		})
		t.Cleanup(func() {
			if err := rcv.Shutdown(ctx); err != nil {
				t.Errorf("receiver shutdown failed: %v", err)
			}
		})

		record, err := sink.waitForMarker(modularMarker, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		assertCapturedLog(t, record, "otel_exec_modular_source", "otel_exec_modular", "otel-exec-modular-host")
	})
}

func setupSplunkTestEnv(t *testing.T, dbPrefix string) (string, string) {
	t.Helper()

	splunkHome := os.Getenv("SPLUNK_HOME")
	if splunkHome == "" {
		splunkHome = "/home/chli/splunk_home"
	}
	if _, err := os.Stat(filepath.Join(splunkHome, "etc")); err != nil {
		t.Skipf("SPLUNK_HOME %q is not available: %v", splunkHome, err)
	}

	splunkDB, err := os.MkdirTemp("", dbPrefix)
	if err != nil {
		t.Fatalf("create SPLUNK_DB: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(splunkDB); err != nil {
			t.Errorf("remove SPLUNK_DB %q: %v", splunkDB, err)
		}
	})

	t.Setenv("SPLUNK_HOME", splunkHome)
	t.Setenv("SPLUNK_DB", splunkDB)
	t.Setenv("HOSTNAME", "otel-exec-test-host")
	return splunkHome, splunkDB
}

func startFrameworkExtension(
	t *testing.T,
	ctx context.Context,
	splunkHome string,
	splunkDB string,
) (component.ID, component.Component) {
	t.Helper()

	frameworkID := component.NewID(component.MustNewType(testFrameworkType))
	factory := splunkframeworkextension.NewFactory()
	cfg := factory.CreateDefaultConfig().(*splunkframeworkextension.Config)
	cfg.SplunkHome = splunkHome
	cfg.SplunkDB = splunkDB

	ext, err := factory.Create(ctx, extension.Settings{
		ID: frameworkID,
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.NewDefaultBuildInfo(),
	}, cfg)
	if err != nil {
		t.Fatalf("create framework extension: %v", err)
	}
	if err := ext.Start(ctx, testHost{}); err != nil {
		t.Fatalf("start framework extension: %v", err)
	}
	t.Cleanup(func() {
		if err := ext.Shutdown(ctx); err != nil {
			t.Errorf("framework extension shutdown failed: %v", err)
		}
	})
	return frameworkID, ext
}

func startExecReceiver(
	t *testing.T,
	ctx context.Context,
	frameworkID component.ID,
	framework component.Component,
	sink *logsSink,
	cfg *Config,
) receiver.Logs {
	t.Helper()

	rcv, err := newLogsReceiver(ctx, receiver.Settings{
		ID: component.NewID(component.MustNewType("splunkexec")),
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.NewDefaultBuildInfo(),
	}, cfg, sink)
	if err != nil {
		t.Fatalf("create exec receiver: %v", err)
	}
	if err := rcv.Start(ctx, testHost{extensions: map[component.ID]component.Component{
		frameworkID: framework,
	}}); err != nil {
		t.Fatalf("start exec receiver: %v", err)
	}
	return rcv
}

func writeScriptedInput(t *testing.T, splunkHome string, marker string) string {
	t.Helper()

	scriptPath := filepath.Join(splunkHome, "bin", "scripts", "otel_exec_scripted_input_"+uniqueSuffix()+".sh")
	if err := os.MkdirAll(filepath.Dir(scriptPath), 0755); err != nil {
		t.Fatalf("create script dir: %v", err)
	}
	body := fmt.Sprintf("#!/bin/sh\nprintf '%s pid=%%s\\n' \"$$\"\n", marker)
	if err := os.WriteFile(scriptPath, []byte(body), 0755); err != nil {
		t.Fatalf("write scripted input: %v", err)
	}
	if err := os.Chmod(scriptPath, 0755); err != nil {
		t.Fatalf("chmod scripted input: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(scriptPath); err != nil && !os.IsNotExist(err) {
			t.Errorf("remove scripted input %q: %v", scriptPath, err)
		}
	})
	return scriptPath
}

func writeModularInputApp(t *testing.T, splunkHome string, scheme string, marker string) {
	t.Helper()

	appDir := filepath.Join(splunkHome, "etc", "apps", "otel_exec_modinput_app_"+uniqueSuffix())
	t.Cleanup(func() {
		if err := os.RemoveAll(appDir); err != nil {
			t.Errorf("remove modular input app %q: %v", appDir, err)
		}
	})

	writeFile(t, filepath.Join(appDir, "default", "app.conf"), ""+
		"[install]\n"+
		"is_configured = 1\n"+
		"\n"+
		"[ui]\n"+
		"is_visible = 0\n"+
		"\n"+
		"[launcher]\n"+
		"version = 1.0.0\n"+
		"author = splunkexecreceiver\n", 0644)
	writeFile(t, filepath.Join(appDir, "README", "inputs.conf.spec"),
		fmt.Sprintf("[%s://<name>]\nmarker = <marker>\n", scheme), 0644)
	writeFile(t, filepath.Join(appDir, "bin", scheme+".sh"), fmt.Sprintf(`#!/bin/sh
if [ "${1:-}" = "--scheme" ]; then
cat <<'EOF'
<scheme>
  <title>OTel Exec Modular Input Test</title>
  <description>Generated by splunkexecreceiver integration test.</description>
  <streaming_mode>xml</streaming_mode>
</scheme>
EOF
exit 0
fi
cat >/dev/null
cat <<'EOF'
<stream>
  <event>
    <data>%s</data>
    <source>otel_exec_modular_source</source>
    <sourcetype>otel_exec_modular</sourcetype>
    <host>otel-exec-modular-host</host>
    <index>main</index>
  </event>
</stream>
EOF
`, marker), 0755)
}

func writeFile(t *testing.T, path string, body string, perm os.FileMode) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create dir for %q: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(body), perm); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
	if perm&0111 != 0 {
		if err := os.Chmod(path, perm); err != nil {
			t.Fatalf("chmod %q: %v", path, err)
		}
	}
}

func assertCapturedLog(t *testing.T, record capturedLog, source string, sourcetype string, host string) {
	t.Helper()

	if record.source != source {
		t.Fatalf("source mismatch: got %q want %q", record.source, source)
	}
	if record.sourcetype != sourcetype {
		t.Fatalf("sourcetype mismatch: got %q want %q", record.sourcetype, sourcetype)
	}
	if record.host != host {
		t.Fatalf("host mismatch: got %q want %q", record.host, host)
	}
}

func uniqueSuffix() string {
	return fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
}
