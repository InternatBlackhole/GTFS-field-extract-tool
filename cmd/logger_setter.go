package cmd

import "github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"

// SetLogger sets the package logger. This is primarily a test helper.
func SetLogger(l logging.Logger) {
	logger = l
}
