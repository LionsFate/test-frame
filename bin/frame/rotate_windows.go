//go:build windows

package main

import (
	"os"
	"sync"

	"github.com/rs/zerolog"
)

// type logWrite struct {{{

type logWrite struct {
	mut sync.RWMutex
	out *os.File // nil = os.Stdout
} // }}}

// func frame.Write {{{

// This is used for writing to the current hourly log file.
//
// Used by zerlog, output is changed by logRotate()
func (lw *logWrite) Write(p []byte) (n int, err error) {
	lw.mut.RLock()
	if lw.out == nil { // os.Stdout, no actual file assigned yet.
		n, err = os.Stdout.Write(p)
	} else {
		n, err = lw.out.Write(p)
	}
	lw.mut.RUnlock()

	return
} // }}}

// func frame.link {{{

func (f *frame) link(fileName string) {
	// Not supported on Windows.
} // }}}

// func frame.newLog {{{

func (f *frame) newLog() zerolog.Logger {
	// New zerolog that outputs to us, through our Write()
	return zerolog.New(&f.lw).With().Timestamp().Logger()
} // }}}

// func frame.logFile {{{

func (f *frame) logFile(lf *os.File) {
	// Rotate the log file.
	f.lw.mut.Lock()
	// Keep the old file in case we need to close it.
	old := f.lw.out
	f.lw.out = lf
	f.lw.mut.Unlock()

	// Now close the old one.
	// If its nil that means we were logging to os.Stdout.
	if old != nil {
		old.Close()
	}
} // }}}
