// +build !windows

package main

import (
	"log"
	"log/syslog"
)

func initLogger(slog bool) {
	if slog {
		lw, err := syslog.New(syslog.LOG_INFO, "cbgb")
		must(err)
		log.SetOutput(lw)
		log.SetFlags(0)
	}
	if *logPlain {
		log.SetFlags(0)
	}
}
