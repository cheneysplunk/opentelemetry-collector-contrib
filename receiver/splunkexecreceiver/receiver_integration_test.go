// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkexecreceiver

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	mu      sync.Mutex
	seen    []capturedLog
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
				s.mu.Lock()
				s.seen = append(s.seen, record)
				s.mu.Unlock()
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
		s.mu.Lock()
		for _, record := range s.seen {
			if strings.Contains(record.body, marker) {
				s.mu.Unlock()
				return record, nil
			}
		}
		s.mu.Unlock()

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

type execFixtureApp struct {
	Name             string
	SourceDir        string
	InstalledAppName string
	InputsConfPath   string
}

func TestSplunkExecReceiverRunsFixtureAppInputsConf(t *testing.T) {
	ctx := context.Background()
	splunkHome, splunkDB := setupSplunkTestEnv(t, "otel_exec_db_")
	removeOldExecTestFixtures(t, splunkHome)

	lineMarker := "otel-python-file-line-" + uniqueSuffix()
	inputPath := filepath.Join(t.TempDir(), "input.txt")
	writeFile(t, inputPath, lineMarker+"\n"+lineMarker+"-second\n", 0644)

	rest := newFakeConfRESTServer(t)
	fixture := execFixtureApp{
		Name:             "Python file modular input",
		SourceDir:        filepath.Join("testdata", "python_file_modinput_app"),
		InstalledAppName: "otel_exec_python_file_app",
		InputsConfPath:   filepath.Join("default", "inputs.conf"),
	}
	inputsConf := installFixtureApp(t, splunkHome, fixture, map[string]string{
		"OTEL_EXEC_TEST_FILE":     inputPath,
		"OTEL_EXEC_TEST_REST_URL": rest.URL,
		"OTEL_EXEC_TEST_MARKER":   lineMarker,
	})

	frameworkID, framework := startFrameworkExtension(t, ctx, splunkHome, splunkDB)
	sink := newLogsSink()
	rcv := startExecReceiver(t, ctx, frameworkID, framework, sink, &Config{
		Framework:  frameworkID,
		InputsConf: inputsConf,
	})
	t.Cleanup(func() {
		if err := rcv.Shutdown(ctx); err != nil {
			t.Errorf("receiver shutdown failed: %v", err)
		}
	})

	record, err := sink.waitForMarker(lineMarker, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertCapturedLog(t, record, "otel_exec_python_file_input_source", "otel_exec_python_file_input", "otel-python-file-host")

	status, err := sink.waitForMarker("otel_exec_python_file_input_rest_status=ok", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status.body, "conf=otel_exec_python_file_test") {
		t.Fatalf("REST status did not include conf name: %q", status.body)
	}
	if !strings.Contains(status.body, "python_executable=") {
		t.Fatalf("REST status did not include python executable: %q", status.body)
	}
	if !strings.Contains(status.body, "python3.9") {
		t.Fatalf("REST status did not use python.required interpreter: %q", status.body)
	}

	rest.AssertCRUD(t)
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

func installFixtureApp(
	t *testing.T,
	splunkHome string,
	fixture execFixtureApp,
	replacements map[string]string,
) string {
	t.Helper()

	if fixture.SourceDir == "" {
		t.Fatal("fixture source dir is required")
	}
	if fixture.InstalledAppName == "" {
		t.Fatal("fixture installed app name is required")
	}
	if fixture.InputsConfPath == "" {
		t.Fatal("fixture inputs.conf path is required")
	}
	if info, err := os.Stat(fixture.SourceDir); err != nil {
		t.Fatalf("%s fixture is not available at %q: %v", fixture.Name, fixture.SourceDir, err)
	} else if !info.IsDir() {
		t.Fatalf("%s fixture source %q is not a directory", fixture.Name, fixture.SourceDir)
	}

	renderedInputsConf := renderFixtureFile(t, filepath.Join(fixture.SourceDir, fixture.InputsConfPath), replacements)
	appDir := filepath.Join(splunkHome, "etc", "apps", fixture.InstalledAppName)
	if err := os.RemoveAll(appDir); err != nil {
		t.Fatalf("remove stale %s fixture app %q: %v", fixture.Name, appDir, err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(appDir); err != nil {
			t.Errorf("remove %s fixture app %q: %v", fixture.Name, appDir, err)
		}
	})

	err := filepath.WalkDir(fixture.SourceDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(fixture.SourceDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dst := filepath.Join(appDir, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0755)
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if filepath.Clean(rel) == filepath.Clean(fixture.InputsConfPath) {
			body = []byte(renderedInputsConf)
		}
		return writeBytes(dst, body, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("install %s fixture app: %v", fixture.Name, err)
	}

	return renderedInputsConf
}

func renderFixtureFile(t *testing.T, path string, replacements map[string]string) string {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture file %q: %v", path, err)
	}

	rendered := string(body)
	for key, value := range replacements {
		rendered = strings.ReplaceAll(rendered, "${"+key+"}", value)
	}
	if strings.Contains(rendered, "${") {
		t.Fatalf("fixture file %q still contains an unresolved placeholder:\n%s", path, rendered)
	}
	return rendered
}

func removeOldExecTestFixtures(t *testing.T, splunkHome string) {
	t.Helper()

	patterns := []string{
		filepath.Join(splunkHome, "bin", "scripts", "otel_exec_scripted_input_*.sh"),
		filepath.Join(splunkHome, "etc", "apps", "otel_exec_modinput_app_*"),
		filepath.Join(splunkHome, "etc", "apps", "otel_exec_gdi_signalfx_app*"),
		filepath.Join(splunkHome, "etc", "apps", "otel_exec_python_file_app*"),
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("glob old exec test fixtures %q: %v", pattern, err)
		}
		for _, match := range matches {
			if err := os.RemoveAll(match); err != nil {
				t.Fatalf("remove old exec test fixture %q: %v", match, err)
			}
		}
	}
}

func writeFile(t *testing.T, path string, body string, perm os.FileMode) {
	t.Helper()

	if err := writeBytes(path, []byte(body), perm); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

func writeBytes(path string, body []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}
	if err := os.WriteFile(path, body, perm); err != nil {
		return err
	}
	if perm&0111 != 0 {
		if err := os.Chmod(path, perm); err != nil {
			return err
		}
	}
	return nil
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

type fakeConfRESTServer struct {
	*httptest.Server
	mu    sync.Mutex
	calls []restCall
	conf  map[string]map[string]string
}

type restCall struct {
	method        string
	path          string
	authorization string
}

func newFakeConfRESTServer(t *testing.T) *fakeConfRESTServer {
	t.Helper()

	server := &fakeConfRESTServer{
		conf: make(map[string]map[string]string),
	}
	server.Server = httptest.NewServer(http.HandlerFunc(server.handle))
	t.Cleanup(server.Close)
	return server
}

func (s *fakeConfRESTServer) handle(w http.ResponseWriter, r *http.Request) {
	call := restCall{
		method:        r.Method,
		path:          r.URL.Path,
		authorization: r.Header.Get("Authorization"),
	}
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()

	if r.URL.Path == "/services/auth/login" && r.Method == http.MethodPost {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte("<response><sessionKey>fake-session</sessionKey></response>"))
		return
	}

	confName, stanza, ok := parseConfPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch {
	case r.Method == http.MethodPost && stanza == "":
		stanza = r.Form.Get("name")
		if stanza == "" {
			http.Error(w, "missing name", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.conf[stanza] = formValuesWithoutName(r.Form)
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodGet && stanza != "":
		s.mu.Lock()
		_, exists := s.conf[stanza]
		s.mu.Unlock()
		if !exists {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"entry":[{"name":%q,"conf":%q}]}`, stanza, confName)))
	case r.Method == http.MethodPost && stanza != "":
		s.mu.Lock()
		if _, exists := s.conf[stanza]; !exists {
			s.conf[stanza] = make(map[string]string)
		}
		for key, values := range r.Form {
			if len(values) > 0 {
				s.conf[stanza][key] = values[0]
			}
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && stanza != "":
		s.mu.Lock()
		delete(s.conf, stanza)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unsupported fake REST operation", http.StatusMethodNotAllowed)
	}
}

func parseConfPath(path string) (string, string, bool) {
	const marker = "/configs/conf-"
	idx := strings.Index(path, marker)
	if idx < 0 {
		return "", "", false
	}
	rest := path[idx+len(marker):]
	parts := strings.SplitN(rest, "/", 2)
	confName, err := url.PathUnescape(parts[0])
	if err != nil || confName == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return confName, "", true
	}
	stanza, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	return confName, stanza, true
}

func formValuesWithoutName(values map[string][]string) map[string]string {
	result := make(map[string]string)
	for key, vals := range values {
		if key == "name" || len(vals) == 0 {
			continue
		}
		result[key] = vals[0]
	}
	return result
}

func (s *fakeConfRESTServer) AssertCRUD(t *testing.T) {
	t.Helper()

	s.mu.Lock()
	calls := append([]restCall(nil), s.calls...)
	s.mu.Unlock()

	expected := []restCall{
		{method: http.MethodPost, path: "/services/auth/login"},
		{method: http.MethodPost, path: "/servicesNS/nobody/system/configs/conf-otel_exec_python_file_test"},
		{method: http.MethodGet, path: "/servicesNS/nobody/system/configs/conf-otel_exec_python_file_test/exec_receiver"},
		{method: http.MethodPost, path: "/servicesNS/nobody/system/configs/conf-otel_exec_python_file_test/exec_receiver"},
		{method: http.MethodDelete, path: "/servicesNS/nobody/system/configs/conf-otel_exec_python_file_test/exec_receiver"},
	}
	for _, want := range expected {
		if !hasRESTCall(calls, want) {
			t.Fatalf("missing REST call %s %s; calls=%v", want.method, want.path, calls)
		}
	}
	for _, call := range calls {
		if call.path == "/services/auth/login" {
			continue
		}
		if call.authorization != "Splunk fake-session" {
			t.Fatalf("REST call %s %s used authorization %q, want Splunk fake-session", call.method, call.path, call.authorization)
		}
	}
}

func hasRESTCall(calls []restCall, want restCall) bool {
	for _, call := range calls {
		if call.method == want.method && call.path == want.path {
			return true
		}
	}
	return false
}
