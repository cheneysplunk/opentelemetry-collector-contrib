// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package splunkframeworkextension initialises the Splunk C++ framework
// (SplunkMainThread / EventLoop) as a process-level OTel Collector extension.
//
// All Splunk-backed receivers and exporters (splunktailreceiver,
// splunktcpoutexporter, …) depend on this extension being listed in the
// collector's service.extensions block.  The extension calls splunkfw_init()
// in Start() — starting a single shared SplunkMainThread — and
// splunkfw_shutdown() in Shutdown() after all pipeline components have stopped.
package splunkframeworkextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension"
