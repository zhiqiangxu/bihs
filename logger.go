package bihs

import (
	"fmt"
	"os"
)

type logger struct {
}

func defaultLogger() Logger {
	return &logger{}
}

func (l *logger) Info(a ...interface{}) {
	fmt.Println(fmt.Sprintln(a...))
}
func (l *logger) Infof(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

func (l *logger) Debug(a ...interface{}) {
	l.Info(a...)
}
func (l *logger) Debugf(format string, a ...interface{}) {
	l.Infof(format, a...)
}
func (l *logger) Fatal(a ...interface{}) {
	l.Info(a...)
	os.Exit(1)
}
func (l *logger) Fatalf(format string, a ...interface{}) {
	l.Infof(format, a...)
	os.Exit(1)
}
func (l *logger) Error(a ...interface{}) {
	l.Info(a...)
}
func (l *logger) Errorf(format string, a ...interface{}) {
	l.Infof(format, a...)
}
