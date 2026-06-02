// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package splunkframeworkextension

/*
#cgo CFLAGS: -I${SRCDIR}

#include "conf_mgmt_cabi.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/splunkframeworkextension/splunkapi"
)

// ConfManager returns the Splunk conf/auth management wrapper. The manager is
// created lazily unless management_port is set, in which case Start creates it
// before launching the REST management surface.
func (e *splunkFrameworkExtension) ConfManager() (splunkapi.ConfManager, error) {
	e.confMu.Lock()
	defer e.confMu.Unlock()

	if e.confMgr != nil {
		return e.confMgr, nil
	}

	confMgr, err := newCConfManager(e.cfg.SplunkHome)
	if err != nil {
		return nil, err
	}
	e.confMgr = confMgr
	return confMgr, nil
}

type cConfManager struct {
	mu sync.Mutex
	h  *C.ConfMgmtHandle
}

type cConfSession struct {
	mu       sync.Mutex
	mgr      *cConfManager
	username string
	s        *C.ConfMgmtSession
}

func newCConfManager(splunkHome string) (*cConfManager, error) {
	if splunkHome == "" {
		return nil, fmt.Errorf("confmgmt: splunk_home must not be empty")
	}

	cHome := C.CString(splunkHome)
	defer C.free(unsafe.Pointer(cHome))

	runtime.LockOSThread()
	handle := C.confmgmt_create(cHome)
	var err error
	if handle == nil {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return nil, fmt.Errorf("confmgmt_create: %w", err)
	}

	return &cConfManager{h: handle}, nil
}

func confMgmtLastError() error {
	msg := C.confmgmt_last_error()
	if msg == nil {
		return fmt.Errorf("unknown error")
	}
	return fmt.Errorf("%s", C.GoString(msg))
}

func validateConfName(confName string) error {
	if confName == "" {
		return fmt.Errorf("confmgmt: conf name must not be empty")
	}
	return nil
}

func validateStanza(stanza string) error {
	if stanza == "" {
		return fmt.Errorf("confmgmt: stanza must not be empty")
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("confmgmt: key must not be empty")
	}
	return nil
}

func (m *cConfManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h != nil {
		C.confmgmt_mgmt_stop(m.h)
		C.confmgmt_destroy(m.h)
		m.h = nil
	}
}

func (m *cConfManager) StartREST(port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("confmgmt: management_port must be between 1 and 65535")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_mgmt_start(m.h, C.int(port))
	var err error
	if rc != 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return fmt.Errorf("confmgmt_mgmt_start: %w", err)
	}
	return nil
}

func (m *cConfManager) RESTRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return false
	}
	return C.confmgmt_mgmt_is_running(m.h) == 1
}

func (m *cConfManager) Get(confName, stanza, key string) (string, bool, error) {
	if err := validateConfName(confName); err != nil {
		return "", false, err
	}
	if err := validateStanza(stanza); err != nil {
		return "", false, err
	}
	if err := validateKey(key); err != nil {
		return "", false, err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))
	cStanza := C.CString(stanza)
	defer C.free(unsafe.Pointer(cStanza))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return "", false, fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	val := C.confmgmt_get(m.h, cConf, cStanza, cKey)
	runtime.UnlockOSThread()
	if val == nil {
		return "", false, nil
	}
	return C.GoString(val), true, nil
}

func (m *cConfManager) Set(confName, stanza, key, value string) error {
	if err := validateConfName(confName); err != nil {
		return err
	}
	if err := validateStanza(stanza); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))
	cStanza := C.CString(stanza)
	defer C.free(unsafe.Pointer(cStanza))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_set(m.h, cConf, cStanza, cKey, cValue)
	var err error
	if rc != 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return fmt.Errorf("confmgmt_set: %w", err)
	}
	return nil
}

func (m *cConfManager) GetStanza(confName, stanza string) (map[string]string, error) {
	if err := validateConfName(confName); err != nil {
		return nil, err
	}
	if err := validateStanza(stanza); err != nil {
		return nil, err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))
	cStanza := C.CString(stanza)
	defer C.free(unsafe.Pointer(cStanza))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return nil, fmt.Errorf("confmgmt: manager is closed")
	}

	text, err := m.readBuffer(func(buf *C.char, bufLen C.size_t) C.int {
		return C.confmgmt_get_stanza(m.h, cConf, cStanza, buf, bufLen)
	})
	if err != nil {
		return nil, fmt.Errorf("confmgmt_get_stanza: %w", err)
	}
	return parseConfKeyValues(text), nil
}

func (m *cConfManager) ListStanzas(confName string) ([]string, error) {
	if err := validateConfName(confName); err != nil {
		return nil, err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return nil, fmt.Errorf("confmgmt: manager is closed")
	}

	text, err := m.readBuffer(func(buf *C.char, bufLen C.size_t) C.int {
		return C.confmgmt_list_stanzas(m.h, cConf, buf, bufLen)
	})
	if err != nil {
		return nil, fmt.Errorf("confmgmt_list_stanzas: %w", err)
	}
	return parseLines(text), nil
}

func (m *cConfManager) DeleteStanza(confName, stanza string) error {
	if err := validateConfName(confName); err != nil {
		return err
	}
	if err := validateStanza(stanza); err != nil {
		return err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))
	cStanza := C.CString(stanza)
	defer C.free(unsafe.Pointer(cStanza))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_delete_stanza(m.h, cConf, cStanza)
	var err error
	if rc != 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return fmt.Errorf("confmgmt_delete_stanza: %w", err)
	}
	return nil
}

func (m *cConfManager) DeleteKey(confName, stanza, key string) error {
	if err := validateConfName(confName); err != nil {
		return err
	}
	if err := validateStanza(stanza); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))
	cStanza := C.CString(stanza)
	defer C.free(unsafe.Pointer(cStanza))
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_delete_key(m.h, cConf, cStanza, cKey)
	var err error
	if rc != 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return fmt.Errorf("confmgmt_delete_key: %w", err)
	}
	return nil
}

func (m *cConfManager) Reload(confName string) error {
	if err := validateConfName(confName); err != nil {
		return err
	}

	cConf := C.CString(confName)
	defer C.free(unsafe.Pointer(cConf))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_reload(m.h, cConf)
	var err error
	if rc != 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return fmt.Errorf("confmgmt_reload: %w", err)
	}
	return nil
}

func (m *cConfManager) Login(username, password string) (splunkapi.ConfSession, error) {
	if username == "" {
		return nil, fmt.Errorf("confmgmt: username must not be empty")
	}

	cUsername := C.CString(username)
	defer C.free(unsafe.Pointer(cUsername))
	cPassword := C.CString(password)
	defer C.free(unsafe.Pointer(cPassword))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.h == nil {
		return nil, fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	session := C.confmgmt_auth_login(m.h, cUsername, cPassword)
	var err error
	if session == nil {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return nil, fmt.Errorf("confmgmt_auth_login: %w", err)
	}

	return &cConfSession{mgr: m, username: username, s: session}, nil
}

func (m *cConfManager) readBuffer(call func(*C.char, C.size_t) C.int) (string, error) {
	bufLen := 4096
	for {
		buf := make([]byte, bufLen)

		runtime.LockOSThread()
		n := call((*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)))
		var err error
		if n < 0 {
			err = confMgmtLastError()
		}
		runtime.UnlockOSThread()
		if err != nil {
			return "", err
		}

		needed := int(n)
		if needed >= len(buf) {
			bufLen = needed + 1
			continue
		}
		return string(buf[:needed]), nil
	}
}

func parseConfKeyValues(text string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return result
}

func parseLines(text string) []string {
	var result []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			result = append(result, line)
		}
	}
	return result
}

func (s *cConfSession) Username() string {
	return s.username
}

func (s *cConfSession) Logout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.s == nil {
		return
	}

	s.mgr.mu.Lock()
	if s.mgr.h != nil {
		C.confmgmt_auth_logout(s.mgr.h, s.s)
	}
	s.mgr.mu.Unlock()
	s.s = nil
}

func (s *cConfSession) CheckCapability(capability string) (bool, error) {
	if capability == "" {
		return false, fmt.Errorf("confmgmt: capability must not be empty")
	}

	cCapability := C.CString(capability)
	defer C.free(unsafe.Pointer(cCapability))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.s == nil {
		return false, fmt.Errorf("confmgmt: session is closed")
	}

	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.h == nil {
		return false, fmt.Errorf("confmgmt: manager is closed")
	}

	runtime.LockOSThread()
	rc := C.confmgmt_auth_check_capability(s.mgr.h, s.s, cCapability)
	var err error
	if rc < 0 {
		err = confMgmtLastError()
	}
	runtime.UnlockOSThread()
	if err != nil {
		return false, fmt.Errorf("confmgmt_auth_check_capability: %w", err)
	}
	return rc == 1, nil
}

func (s *cConfSession) Roles() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.s == nil {
		return nil, fmt.Errorf("confmgmt: session is closed")
	}

	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.h == nil {
		return nil, fmt.Errorf("confmgmt: manager is closed")
	}

	text, err := s.mgr.readBuffer(func(buf *C.char, bufLen C.size_t) C.int {
		return C.confmgmt_auth_get_roles(s.mgr.h, s.s, buf, bufLen)
	})
	if err != nil {
		return nil, fmt.Errorf("confmgmt_auth_get_roles: %w", err)
	}
	return parseLines(text), nil
}
