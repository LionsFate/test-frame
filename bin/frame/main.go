package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"frame/cmanager"
	"frame/cmerge"
	"frame/idmanager"
	"frame/imgproc"
	"frame/render"
	"frame/tagmanager"
	"frame/types"
	"frame/weighter"
	"frame/yconf"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// func usage {{{

func usage() {
	fmt.Printf("usage: %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(-1)
} // }}}

// type confFile struct {{{

// Note that at least one of the optional services must be enabled.
//
// Those being: ImageProc, CacheMerge or Weighter.
type confFile struct {
	// File/Path to the TagManager configuration, passed in to tagmanager.New()
	//
	// This one is not optional.
	TagManager string `yaml:"tagmanager"`

	// Maps hashes <-> IDs (uint64).
	//
	// This one is not optional.
	IDManager string `yaml:"idmanager"`

	// Configuration path for ImageProc.
	//
	// Optional - If left empty ImageProc will not be loaded.
	ImageProc string `yaml:"imageproc"`

	// Configuration path for CMerge
	//
	// Optional - If left empty CMerge will not be loaded.
	CacheMerge string `yaml:"cachemerge"`

	// Configure the CacheManager.
	//
	// Required if either ImageProc or Renderer is configured.
	CacheManager string `yaml:"cachemanager"`

	// Configure path for Weighter
	//
	// Optional - If left empty Weighter will not be loaded.
	Weighter string `yaml:"weighter"`

	// Configure path for Render.
	//
	// Optional - If left empty Render will not be loaded.
	//
	// Requires Weighter and CacheManager.
	Render string `yaml:"render"`

	// The path for the hourly log file to be written.
	// STDOUT and STDERR will be redirected to this file.
	//
	// Optional - If left empty then STDOUT and STDERR will get all output.
	LogPath string `yaml:"logpath"`
} // }}}

// type frame struct {{{

type frame struct {
	l     zerolog.Logger
	cFile string
	co    *confFile
	tm    types.TagManager
	im    types.IDManager
	ip    *imgproc.ImageProc
	cm    *cmerge.CMerge
	cma   *cmanager.CManager
	we    types.Weighter
	re    *render.Render
	yc    *yconf.YConf
	ctx   context.Context
	can   context.CancelFunc

	// We rotate our log file hourly.
	//
	// These handle the logic for that.
	curHour int32        // Access only using atomics.
	out     atomic.Value // io.WriteCloser
} // }}}

var pathsConf = yconf.Callers{
	Empty: func() interface{} { return &confFile{} },
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

// func frame.close {{{

func (f *frame) close() {
	// Signal it all to shutdown.
	f.can()

	f.l.Info().Msg("Shutting down")

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
	}

	// Get our shutdown context
	f.ctx, f.can = context.WithCancel(context.Background())

	// New zerolog that we share with everyone.
	//
	// This function handles differences between different systems.
	f.l = f.newLog()

	// Lets load our flags.
	flag.StringVar(&f.cFile, "conf", "", "YAML Configuration directory")
	flag.Parse()

	if f.cFile == "" {
		usage()
	}

	f.yc, err = yconf.New(f.cFile, pathsConf, &f.l, f.ctx)
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
	f.tm, err = tagmanager.New(f.co.TagManager, &f.l, f.ctx)
	if err != nil {
		f.l.Err(err).Msg("TagManager")
		f.tm = nil
		f.close()
		os.Exit(-1)
	}

	if f.co.IDManager == "" {
		f.l.Error().Msg("Missing idmanager configuration")
		os.Exit(-1)
	}

	f.im, err = idmanager.New(f.co.IDManager, &f.l, f.ctx)
	if err != nil {
		f.l.Err(err).Msg("IDManager")
		f.im = nil
		f.close()
		os.Exit(-1)
	}

	if f.co.CacheManager != "" {
		f.cma, err = cmanager.New(f.co.CacheManager, f.im, &f.l, f.ctx)
		if err != nil {
			f.cma = nil
			f.l.Err(err).Msg("CacheManager")
			f.close()
			os.Exit(-1)
		}
	}

	// Do we load the ImageProc?
	if f.co.ImageProc != "" {
		if f.cma == nil {
			f.l.Err(errors.New("imageproc requires cachemanager")).Send()
			f.close()
			os.Exit(-1)
		}

		// And next is our real core, the one doing all the real work here, ImageProc.
		f.ip, err = imgproc.New(f.co.ImageProc, f.tm, f.cma, &f.l, f.ctx)
		if err != nil {
			f.ip = nil
			f.l.Err(err).Msg("ImageProc")
			f.close()
			os.Exit(-1)
		}
	}

	// Load CacheMerge?
	if f.co.CacheMerge != "" {
		f.cm, err = cmerge.New(f.co.CacheMerge, f.tm, &f.l, f.ctx)
		if err != nil {
			f.cm = nil
			f.l.Err(err).Msg("CMerge")
			f.close()
			os.Exit(-1)
		}
	}

	// Load the Weighter?
	if f.co.Weighter != "" {
		f.we, err = weighter.New(f.co.Weighter, f.tm, &f.l, f.ctx)
		if err != nil {
			f.cm = nil
			f.l.Err(err).Msg("Weighter")
			f.close()
			os.Exit(-1)
		}
	}

	if f.co.Render != "" {
		if f.we == nil {
			f.l.Err(errors.New("render requires weighter")).Send()
			f.close()
			os.Exit(-1)
		}

		if f.cma == nil {
			f.l.Err(errors.New("render requires cachemanager")).Send()
			f.close()
			os.Exit(-1)
		}

		f.re, err = render.New(f.co.Render, f.we, f.cma, &f.l, f.ctx)
		if err != nil {
			f.re = nil
			f.l.Err(err).Msg("Render")
			f.close()
			os.Exit(-1)
		}
	}

	f.l.Info().Msg("Startup Finished")

	// Now we just wait until something tells us to shutdown.
	f.Wait()

	f.l.Info().Msg("Shutting down")
	f.close()
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

	ctx := f.ctx

	for {
		select {
		case <-tick.C:
			// Ok, we do actually rotate log files.
			//
			// We can go a while without actually logging anything.
			// With that in mind its important to ensure we rotate the log file.
			hour := int32(time.Now().Hour())

			// logRotate() will update curHour for us.
			if hour != atomic.LoadInt32(&f.curHour) {
				fl.Debug().Msg("rotate")
				if err := f.logRotate(); err != nil {
					f.l.Err(err).Msg("rotate")
				}
			}
		case _, ok := <-ctx.Done():
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

	// If the hour has not changed, nothing to do.
	if hour == atomic.LoadInt32(&f.curHour) {
		return nil
	}

	path := f.co.LogPath
	fileName := "frame." + now.Format("2006-01-02.15") + ".log"
	fullName := path + "/" + fileName

	lf, err := os.OpenFile(fullName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	fl.Debug().Msg("rotating logfile")

	// This will close the file for us.
	f.logFile(lf)

	// Switch the hour
	atomic.StoreInt32(&f.curHour, hour)

	// Create the symlink if needed.
	// Does nothing on Windows.
	f.link(fileName)

	return nil
} // }}}
