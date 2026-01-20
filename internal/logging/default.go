package logging

var logger Logger

// SetLogger sets the package logger. This is primarily a test helper.
func SetLogger(l Logger) {
	logger = l
}

func SetNewLoggerWithLevel(level StatusLevel) {
	logger = NewDefaultLogger(level)
}

func GetLogger() Logger {
	if logger == nil {
		panic("Trying to get nil logger!")
	}
	return logger
}
