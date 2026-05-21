// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkframeworkextension

import (
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

const nativeInputsConfSentinel = "[default]\n"

// NewMonitorPipeline creates a TailManager-backed input pipeline from the
// extension-owned inputs.conf cache. The sentinel stanza tells the generic CABI
// to create an input pipeline without injecting programmatic monitor stanzas;
// the native TailManager then reads the merged PropertyPages cache directly.
func (e *splunkFrameworkExtension) NewMonitorPipeline() (splunkapi.Pipeline, error) {
	mgr, err := e.ConfManager()
	if err != nil {
		return nil, err
	}

	monitorCount, err := enabledMonitorCount(mgr)
	if err != nil {
		return nil, err
	}
	if monitorCount == 0 {
		return nil, fmt.Errorf("splunkframeworkextension: no enabled monitor:// stanzas found in inputs.conf")
	}

	if err := e.claimMonitorPipeline(); err != nil {
		return nil, err
	}

	p, err := e.NewPipeline(splunkapi.PipelineConfig{
		InputsConf: nativeInputsConfSentinel,
	})
	if err != nil {
		e.releaseMonitorPipeline()
		return nil, err
	}

	e.logger.Info("created monitor input pipeline from inputs.conf",
		zap.Int("enabled_monitor_stanzas", monitorCount),
	)
	return &monitorPipeline{Pipeline: p, release: e.releaseMonitorPipeline}, nil
}

func (e *splunkFrameworkExtension) claimMonitorPipeline() error {
	e.monitorMu.Lock()
	defer e.monitorMu.Unlock()

	if e.monitorActive {
		return fmt.Errorf("splunkframeworkextension: monitor input pipeline is already active; configure only one splunktailreceiver")
	}
	e.monitorActive = true
	return nil
}

func (e *splunkFrameworkExtension) releaseMonitorPipeline() {
	e.monitorMu.Lock()
	defer e.monitorMu.Unlock()
	e.monitorActive = false
}

type monitorPipeline struct {
	splunkapi.Pipeline
	release func()
	once    sync.Once
}

func (p *monitorPipeline) Destroy() {
	p.Pipeline.Destroy()
	p.once.Do(p.release)
}

func enabledMonitorCount(mgr splunkapi.ConfManager) (int, error) {
	stanzas, err := mgr.ListStanzas("inputs")
	if err != nil {
		return 0, err
	}

	count := 0
	for _, stanza := range stanzas {
		if !strings.HasPrefix(stanza, "monitor:") {
			continue
		}
		values, err := mgr.GetStanza("inputs", stanza)
		if err != nil {
			return 0, err
		}
		if confBool(values["disabled"]) {
			continue
		}
		count++
	}
	return count, nil
}

func confBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	default:
		return false
	}
}
