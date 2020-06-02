package log

import (
	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})

	Debug(args ...interface{})
	Info(args ...interface{})
	Print(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})

	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Println(args ...interface{})
	Warnln(args ...interface{})
	Errorln(args ...interface{})
	Fatalln(args ...interface{})
	Panicln(args ...interface{})

	WithField(key string, value interface{}) *logrus.Entry
}

func NewLogrus() *logrus.Logger {
	log := logrus.New()
	log.AddHook(filename.NewHook())
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(Filter{log.Out, [][]byte{
		[]byte("error when reading request headers:"),
	}})
	return log
}

func SetLevelLogrus(logger *logrus.Logger, levelStr string) *logrus.Logger {
	// Log Level
	level, err := logrus.ParseLevel(levelStr)
	if err != nil {
		logger.Fatalf(`Invalid log level "%s"`, level)
	}
	logger.SetLevel(level)
	return logger
}
