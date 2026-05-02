// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

// Package splunktcpoutexporter implements a Splunk S2S TCP output exporter that
// forwards OTel log records to a Splunk indexer via the native S2S protocol,
// using the libtcpout_cabi.so C-ABI shared library built from Splunk's tcpout
// subsystem.
//
// Build constraint: Linux only (requires CGo and libtcpout_cabi.so).
//
// Build:
//
//	CGO_CFLAGS="-I/path/to/tcpout_lib" \
//	CGO_LDFLAGS="-L/path/to/tcpout_lib -L/path/to/splunk_home/lib \
//	             -Wl,-rpath,/path/to/tcpout_lib \
//	             -Wl,-rpath,/path/to/splunk_home/lib" \
//	go build ./...
package splunktcpoutexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunktcpoutexporter"
