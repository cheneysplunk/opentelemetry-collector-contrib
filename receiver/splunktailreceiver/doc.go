// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package splunktailreceiver implements a Splunk tail-input receiver that
// obtains a native TailManager pipeline from splunkframeworkextension and emits
// raw file chunks as OTel log records.
//
// Build constraint: Linux only because the paired splunkframeworkextension owns
// the Splunk CABI and runtime linkage.
package splunktailreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunktailreceiver"
