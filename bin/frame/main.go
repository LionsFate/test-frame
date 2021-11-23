package main

import (
	"flag"
	"fmt"
	"frame/cmerge"
	"frame/imgproc"
	"frame/tagmanager"
	"frame/types"
	"frame/yconf"
	"github.com/rs/zerolog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// func usage {{{

func usage() {
	fmt.Printf("usage: %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(-1)
} // }}}

type confFile struct {
	// File/Path to the TagManager configuration, passed in to tagmanager.New()
	//
	// This one is not optional.
	TagManager string `yaml:"tagmanager"`

	// Configuration path for ImageProc.
	//
	// Optional - If left empty, ImageProc will not be loaded.
	ImageProc string `yaml:"imageproc"`

	// Configuration path for CMerge
	//
	// Optional - If left empty, CMerge will not be loaded.
	CacheMerge string `yaml:"cachemerge"`

	// The path for the hourly log file to be written.
	// STDOUT and STDERR will be redirected to this file.
	//
	// Optional - If left empty then STDOUT and STDERR will get all output.
	LogPath string `yaml:"logpath"`
}

type frame struct {
	l       zerolog.Logger
	cFile   string
	co      *confFile
	tm      types.TagManager
	ip      *imgproc.ImageProc
	cm      *cmerge.CMerge
	curHour int32
	yc      *yconf.YConf
	bye     chan struct{}
}

// func emptyConf {{{

func emptyConf() interface{} {
	return &confFile{}
} // }}}

var pathsConf = yconf.Callers{
	Empty: emptyConf,
}

// func frame.Wait {{{

// Does not return until a signal such as SIGTERM, SIGINT or SIGQUIT.
func (f *frame) Wait() {
	fl := f.l.With().Str("func", "Wait").Logger()

	// And now we just loop waiting for a signal.
	endSig := make(chan os.Signal)
	signal.Notify(endSig, os.Interrupt, syscall.SIGTERM)

	fl.Info().Msg("Waiting on signal")

	// Wait for a signal ...
	<-endSig

	signal.Stop(endSig)
} // }}}

// func frame.Close {{{

func (f *frame) Close() {
	// Stop the log rotation goroutine.
	close(f.bye)

	f.l.Info().Msg("Shutting down")

	// We can be called in the middle of startup, as well as with only certain modules loaded.
	//
	// Always nil check to know what we need to shutdown.
	if f.ip != nil {
		f.ip.Close()
	}

	if f.cm != nil {
		f.cm.Close()
	}

	if f.tm != nil {
		f.tm.Close()
	}

	// This time delay gives the above just a little more time to shutdown properly.
	time.Sleep(300 * time.Millisecond)
} // }}}

// func main {{{

func main() {
	var err error

	// Set the time logging format
	zerolog.TimeFieldFormat = time.RFC3339

	f := &frame{
		// Set to an invalid hour to ensure it rotates the first time.
		curHour: 50,
		bye:     make(chan struct{}, 0),
	}

	// As this program is meant to start, run, and then stop - Not do anything in the background, we just use our own YAML configuration.
	f.l = zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Lets load our flags.
	flag.StringVar(&f.cFile, "conf", "", "YAML Configuration directory")
	flag.Parse()

	if f.cFile == "" {
		usage()
	}

	f.yc, err = yconf.New(f.cFile, pathsConf, &f.l)
	if err != nil {
		f.l.Err(err).Msg("yconf.New")
		os.Exit(-1)
	}

	if err = f.yc.CheckConf(); err != nil {
		f.l.Err(err).Msg("yc.CheckConf")
		os.Exit(-1)
	}

	f.l.Debug().Interface("conf", f.yc.Get()).Send()

	// Get the loaded configuration
	if lconf, ok := f.yc.Get().(*confFile); ok {
		f.co = lconf
	}

	if f.co == nil {
		f.l.Err(nil).Msg("No paths loaded from configuration")
		os.Exit(-1)
	}

	if f.co.LogPath != "" {
		if err := f.logRotate(); err != nil {
			f.l.Err(err).Msg("rotate")
			os.Exit(-1)
		}

		// Log rotation good.
		go f.logLoopy()
	}

	f.l.Debug().Interface("yc", f.co).Send()

	if f.co.TagManager == "" {
		f.l.Error().Msg("Missing tagmanager configuration")
		os.Exit(-1)
	}

	// Now we need the TagManager.
	f.tm, err = tagmanager.New(f.co.TagManager, &f.l)
	if err != nil {
		f.l.Err(err).Msg("TagManager")
		f.tm = nil
		f.Close()
		os.Exit(-1)
	}

	// Do we load the ImageProc?
	if f.co.ImageProc != "" {
		// And next is our real core, the one doing all the real work here, ImageProc.
		f.ip, err = imgproc.New(f.co.ImageProc, f.tm, &f.l)
		if err != nil {
			f.ip = nil
			f.l.Err(err).Msg("ImageProc")
			f.Close()
			os.Exit(-1)
		}
	}

	if f.co.CacheMerge != "" {
		f.cm, err = cmerge.New(f.co.CacheMerge, f.tm, &f.l)
		if err != nil {
			f.cm = nil
			f.l.Err(err).Msg("CMerge")
			f.Close()
			os.Exit(-1)
		}
	}

	f.l.Info().Msg("Startup Finished")

	// Now we just wait until something tells us to shutdown.
	f.Wait()

	f.l.Info().Msg("Shutting down")
	f.Close()
} // }}}

// func frame.logLoopy {{{

// This handles log rotation for us.
//
// Every minute it checks to see if the hour changes, and if so it rotates the file and sets STDOUT and STDERR for us.
func (f *frame) logLoopy() {
	fl := f.l.With().Str("func", "logLoopy").Logger()

	// Basic tracking ticker, runs every minute.
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			// Ok, we do actually rotate log files.
			//
			// We can go a while without actually logging anything.
			// With that in mind its important to ensure we rotate the log file.
			hour := int32(time.Now().Hour())

			// logRotate() will update curHour for us.
			if hour != f.curHour {
				fl.Debug().Msg("rotate")
				if err := f.logRotate(); err != nil {
					f.l.Err(err).Msg("rotate")
				}
			}
		case _, ok := <-f.bye:
			if !ok {
				return
			}
		}
	}
} // }}}

// func frame.logRotate {{{

func (f *frame) logRotate() error {
	fl := f.l.With().Str("func", "logRotate").Logger()

	now := time.Now()
	hour := int32(now.Hour())
	path := f.co.LogPath

	// If the hour has not changed, nothing to do.
	if hour == f.curHour {
		return nil
	}

	// Make the log file name.
	fileName := "frame." + now.Format("2006-01-02.15") + ".log"
	fullName := path + "/" + fileName

	lf, err := os.OpenFile(fullName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	fl.Debug().Msg("rotating logfile")

	// Now replace STDOUT and STDERR, which is what the log file actually points to.
	fd := int(lf.Fd())
	syscall.Dup2(fd, 1)
	syscall.Dup2(fd, 2)

	// And now we can close the original file we opened
	lf.Close()

	// Switch the hour
	f.curHour = hour

	// Is there a link?
	linkFile := path + "/frame.current"

	// Create our new temporary symlink
	if err := os.Symlink(fileName, linkFile+".tmp"); err != nil {
		fl.Err(err).Msg("Symlink")
		return err
	}

	// Atomic rename
	os.Rename(linkFile+".tmp", linkFile)

	return nil
} // }}}
