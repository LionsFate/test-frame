//go:build !windows

package main

import (
	"os"
	"syscall"

	"github.com/rs/zerolog"
)

// func frame.link {{{

func (f *frame) link(fileName string) {
	path := f.co.LogPath

	// Is there a link?
	linkFile := path + "/frame.current"

	// Create our new temporary symlink
	if err := os.Symlink(fileName, linkFile+".tmp"); err != nil {
		fl.Err(err).Msg("Symlink")
		return err
	}

	// Atomic rename
	os.Rename(linkFile+".tmp", linkFile)
} // }}}

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

	// Original file no longer needed.
	lf.Close()
} // }}}
