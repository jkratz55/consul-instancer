package instancer

import (
	"fmt"

	"go.uber.org/zap"
)

type Logger interface {
	Log(template string, args ...interface{})
}

type LoggerFunc func(string, ...interface{})

func (l LoggerFunc) Log(template string, args ...interface{}) {
	l(template, args...)
}

type NopLogger struct{}

func (n NopLogger) Log(template string, args ...interface{}) {}

func ZapWrapper(logger *zap.Logger) Logger {
	return LoggerFunc(func(s string, i ...interface{}) {
		logger.Info(fmt.Sprintf(s, i...))
	})
}
