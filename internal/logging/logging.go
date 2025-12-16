package logging

type StatusLevel int

const (
	NoStatus StatusLevel = iota
	Verbose
	EvenMoreVerbose
)

type LogConsumer func(status string, statusLevel StatusLevel)

type LogReporter func(level StatusLevel, format string, a ...any)
