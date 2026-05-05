// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package splunktailreceiver implements a Splunk tail-input receiver that
// watches log files using the native Splunk TailReader / TailWatcher pipeline
// (via libtailinput_cabi.so) and emits each line-broken event as an OTel
// log record.
//
// Build constraint: Linux only (requires CGo and libtailinput_cabi.so).
//
// Build:
//
//	CGO_CFLAGS="-I/path/to/tail_lib" \
//	CGO_LDFLAGS="-L/path/to/tail_lib -L/path/to/splunk_home/lib \
//	             -Wl,-rpath,/path/to/tail_lib \
//	             -Wl,-rpath,/path/to/splunk_home/lib" \
//	go build ./...
package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"
