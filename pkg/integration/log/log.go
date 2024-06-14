package log

import (
	"encoding/json"
	"os"

	logrus "github.com/sirupsen/logrus"
)

var RootLogger = logrus.New()

func init() {
	RootLogger.SetLevel(logrus.WarnLevel)
	RootLogger.SetOutput(os.Stderr)
}

func Debugf(format string, args ...interface{}) {
	RootLogger.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	RootLogger.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	RootLogger.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	RootLogger.Errorf(format, args...)
}

func Fatalf(err error) {
	RootLogger.Fatalf("FATAL: can't continue: %v", err)
	os.Exit(1)
}

func IsDebugEnabled() bool {
	return RootLogger.GetLevel() == logrus.DebugLevel
}

func PrettyPrintJson(val any) error {
	enc := json.NewEncoder(RootLogger.Out)
	enc.SetIndent("", "  ")
	return enc.Encode(val)
}
