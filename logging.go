package gocouchbaseio

import (
	"fmt"
)

type Logger interface {
	Output(s string) error
}

var (
	globalLogger Logger
)

func SetLogger(logger Logger) {
	globalLogger = logger
}

func logDebugf(format string, v ...interface{}) {
	if globalLogger != nil {
		globalLogger.Output(fmt.Sprintf("DEBUG: "+format, v...))
	}
}
