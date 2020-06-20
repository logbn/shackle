package monitor

import (
	"strings"

	"highvolume.io/shackle/log"
)

type logger struct {
	app string
	log log.Logger
}

func (m *logger) Count(name, err string, labels ...string) {
	var fn = m.log.Infof
	if strings.HasPrefix(name, "warn_") {
		fn = m.log.Warnf
	} else if strings.HasPrefix(name, "error_") {
		fn = m.log.Errorf
	} else if strings.HasPrefix(name, "info_") {
		fn = m.log.Infof
	} else {
		return
	}
	fn("%s - %s - %s (%s) ", m.app, name, err, strings.Join(labels, ", "))
}

func (m *logger) CountN(name, err string, n int, labels ...string) {
	var fn = m.log.Debugf
	if strings.HasPrefix(name, "warn_") {
		fn = m.log.Warnf
	} else if strings.HasPrefix(name, "error_") {
		fn = m.log.Errorf
	} else if strings.HasPrefix(name, "info_") {
		fn = m.log.Infof
	} else {
		return
	}
	fn("%s - %s - %s (%s) x%d", m.app, name, err, strings.Join(labels, ", "), n)
}

func (m *logger) Histogram(name string, v float64, labels ...string) {
	var fn = m.log.Debugf
	if strings.HasPrefix(name, "warn_") {
		fn = m.log.Warnf
	} else if strings.HasPrefix(name, "error_") {
		fn = m.log.Errorf
	} else if strings.HasPrefix(name, "info_") {
		fn = m.log.Errorf
	}
	fn("%s - %s (%s) hist %f", m.app, name, strings.Join(labels, ", "), v)
}

func (m *logger) Summary(name string, v float64, labels ...string) {
	var fn = m.log.Debugf
	if strings.HasPrefix(name, "warn_") {
		fn = m.log.Warnf
	} else if strings.HasPrefix(name, "error_") {
		fn = m.log.Errorf
	} else if strings.HasPrefix(name, "info_") {
		fn = m.log.Errorf
	}
	fn("%s - %s (%s) summary %f", m.app, name, strings.Join(labels, ", "), v)
}

func (m *logger) AgentStart() {}

func (m *logger) AgentStop() {}
