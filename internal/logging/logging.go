package logging

import (
	"log"
)

type StatusLevel int

const (
	NoStatus StatusLevel = iota
	Verbose
	EvenMoreVerbose
)

// Deprecated: use DefaultLogger or implement Logger interface instead
type LogConsumer func(status string, statusLevel StatusLevel)

// Deprecated: use DefaultLogger or implement Logger interface instead
type LogReporter func(level StatusLevel, format string, a ...any)

type Logger interface {
	Info(format string, a ...any)
	Verbose(format string, a ...any)
	EvenMoreVerbose(format string, a ...any)
	Error(format string, a ...any)
	Log(level StatusLevel, format string, a ...any)
}

type DefaultLogger struct {
	reportLevel StatusLevel
	logger      *log.Logger
}

func NewDefaultLogger(reportLevel StatusLevel) *DefaultLogger {
	return NewLogger(reportLevel, log.Default())
}

func NewLogger(reportLevel StatusLevel, logger *log.Logger) *DefaultLogger {
	return &DefaultLogger{
		reportLevel: reportLevel,
		logger:      logger,
	}
}

func (l *DefaultLogger) Log(level StatusLevel, format string, a ...any) {
	if l.reportLevel >= level {
		p := make([]any, len(a))
		for i, v := range a {
			switch v := v.(type) {
			case func() any:
				p[i] = v()
			case func() []any:
				p[i] = v()
			default:
				p[i] = v
			}
		}
		l.logger.Printf(format, p...)
	}
}

func (l *DefaultLogger) Verbose(format string, a ...any) {
	l.Log(Verbose, format, a...)
}

func (l *DefaultLogger) EvenMoreVerbose(format string, a ...any) {
	l.Log(EvenMoreVerbose, format, a...)
}

func (l *DefaultLogger) Info(format string, a ...any) {
	l.Log(NoStatus, format, a...)
}

func (l *DefaultLogger) Error(format string, a ...any) {
	l.Log(NoStatus, format, a...)
}
