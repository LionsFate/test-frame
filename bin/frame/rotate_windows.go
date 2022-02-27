//go:build windows

package main

import (
	"time"
	"os"

	"github.com/rs/zerolog"
)

// func frame.Write {{{

// This is used for writing to the current hourly log file.
//
// Used by zerlog, output is changed by logRotate()
func (f *frame) Write(p []byte) (n int, err error) {
	// Get the output file.
	w := f.out.Load().(*os.File)
	return w.Write(p)
} // }}}

// func frame.link {{{

func (f *frame) link(fileName string) {
	// Not supported on Windows.
} // }}}

// func frame.newLog {{{

func (f *frame) newLog() zerolog.Logger {
	// Set our output to STDOUT by default.
	f.out.Store(os.Stdout)

	// New zerolog that outputs to us, through our Write()
	return zerolog.New(f).With().Timestamp().Logger()
} // }}}

// func frame.logFile {{{

func (f *frame) logFile(lf *os.File) {
	// We need the old file first to save.
	old := f.out.Load().(*os.File)

	// And save the new one.
	f.out.Store(lf)

	// Now close the old one (provided its not STDOUT).
	if old != os.Stdout {
		go func() {
			// Give it about 10 seconds before we close out the old file.
			time.Sleep(10*time.Second)
			old.Close()
		}()
	}

	// We do not close lf here, it will be closed when the log file rotates.
} // }}}
