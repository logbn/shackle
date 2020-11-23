package mock

import (
	"fmt"
	"strings"
	"sync"

	"logbin.io/shackle/log"
)

// Mock Logger
type Logger struct {
	log.Logger
	Errors []string
	Warns  []string
	Infos  []string
	Debugs []string
	mutex  sync.Mutex
}

func (l *Logger) Warn(msg ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Warns = append(l.Warns, fmt.Sprintf("%v", msg))
}
func (l *Logger) Info(msg ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Infos = append(l.Infos, fmt.Sprintf("%v", msg))
}
func (l *Logger) Error(msg ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Errors = append(l.Errors, fmt.Sprintf("%v", msg))
}
func (l *Logger) Warnf(msg string, args ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Warns = append(l.Warns, fmt.Sprintf(msg, args...))
}
func (l *Logger) Infof(msg string, args ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Infos = append(l.Infos, fmt.Sprintf(msg, args...))
}
func (l *Logger) Debugf(msg string, args ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Debugs = append(l.Debugs, fmt.Sprintf(msg, args...))
}
func (l *Logger) Errorf(msg string, args ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Errors = append(l.Errors, fmt.Sprintf(msg, args...))
}
func (l *Logger) String() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return strings.Join([]string{
		strings.Join(l.Errors, "\n"),
		strings.Join(l.Warns, "\n"),
		strings.Join(l.Infos, "\n"),
		strings.Join(l.Debugs, "\n"),
	}, "\n")
}
func (l *Logger) Reset() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.Errors = []string{}
	l.Warns = []string{}
	l.Infos = []string{}
	l.Debugs = []string{}
}

// Thread safe record accessors
func (l *Logger) GetErrors() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.Errors
}
func (l *Logger) GetWarns() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.Warns
}
func (l *Logger) GetInfos() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.Infos
}
func (l *Logger) GetDebugs() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.Debugs
}
