//go:build !windows

package main

import (
	"os"
	"syscall"

	"github.com/rs/zerolog"
)

// func frame.newLog {{{

func (f *frame) newLog() zerolog.Logger {
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
} // }}}

// func frame.logFile {{{

func (f *frame) logFile(lf *os.File) {
	// Replace STDOUT and STDERR, which is what the log file actually points to.
	fd := int(lf.Fd())
	syscall.Dup2(fd, 1)
	syscall.Dup2(fd, 2)
} // }}}
