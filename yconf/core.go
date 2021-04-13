// YAML configuration for Frame.
package yconf

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// func New {{{

// Creates and returns a new *YConf, though it does not yet start to parse the configuration files.
//
//
// For background loading and watching, use Start().
// If you only want to manually check the configuration, use CheckConfig().
//
// To know when loading has finished in the background ensure you have Callers.Notify set.
func New(confPath string, ca Callers, l *zerolog.Logger) (*YConf, error) {
	yc := &YConf{
		confPath: confPath,
		ca:       ca,
		bye:      make(chan struct{}, 0),

		// So we never have a situation where lo is nil
		lo: &loaded{},

		l: l.With().Str("mod", "yconf").Logger(),
	}

	fl := yc.l.With().Str("func", "New").Logger()
	fl.Debug().Msg("Created")

	return yc, nil
} // }}}

// func YConf.Start {{{

// Loads and processes all the configuration files in the path provided.
//
// Ensures that all the configuration files are loaded properly before starting the background monitoring.
// If an error is returned then no background monitoring started, and is safe to call again if the problem is fixed.
//
// Note that you can skip calling this and thus not need to call Stop(), and only manually
// check the configuration with CheckConf() whenever you want.
func (yc *YConf) Start() error {
	fl := yc.l.With().Str("func", "Start").Logger()
	fl.Debug().Msg("Started")

	if err := yc.CheckConf(); err != nil {
		fl.Err(err).Msg("CheckConf")
		return err
	}

	go yc.loopy()

	// Looks like we have everything loaded fine.
	return nil
} // }}}

// func YConf.Stop {{{

// This shuts down Conf and any background goroutines running.
//
// Should only be called if you have called Start() prior.
func (yc *YConf) Stop() {
	close(yc.bye)

	fl := yc.l.With().Str("func", "Stop").Logger()
	fl.Debug().Msg("Stopped")
} // }}}

// func YConf.isLoadedEqual {{{

// This compares the already loaded lo with the newly loaded one and returns if the two are equal or not.
//
// If this is the first load then this is rather quick and just returns false.
//
// Note that is there is no Changed function, this always returns false (since it has no way to tell).
func (yc *YConf) isLoadedEqual(nlo *loaded) bool {
	// While we are comparing the currently loaded
	yc.loMut.RLock()
	defer yc.loMut.RUnlock()

	// First time loaded?
	if yc.lo == nil {
		return false
	}

	if yc.ca.Changed == nil {
		return false
	}

	return !yc.ca.Changed(yc.lo, nlo)
} // }}}

// func YConf.reload {{{

// Attempts to reload the configuration files in the event of changes.
//
// Assumes the caller already has at least a read lock on paMut.
//
// Send notifications if the files did change.
//
// In the event if an error, previously loaded configuration is used and no notifications are sent.
func (yc *YConf) reload() error {
	fl := yc.l.With().Str("func", "reload").Logger()

	lo := &loaded{
		newest: time.Now(),
	}

	fl.Debug().Msg("Checking for changes")

	// Load all our various files.
	if err := yc.loadConf(lo, yc.confPath); err != nil {
		fl.Err(err).Str("conf", yc.confPath).Msg("loadConf")
		return fmt.Errorf("loadConf(%s): %w", yc.confPath, err)
	}

	// Now has anything actually changed?
	//
	// If not we do not send notifications or replace lo, though we do update our timestamp.
	if yc.isLoadedEqual(lo) {
		fl.Debug().Msg("unchanged")

		// Get a quick lock so we can update the timestamp on the currently loaded lo.
		//
		// If we didn't do this then whenever someone changed the timestamp on the files, but not the content, we would be
		// constantly reloading the files and detecting no changes. This ensures we ignore the files until the timestamp changes again.
		yc.loMut.Lock()
		yc.lo.newest = lo.newest
		yc.loMut.Unlock()
		return nil
	}

	// Everything looks good, so set the new loaded.
	yc.loMut.Lock()
	yc.lo = lo
	yc.loMut.Unlock()

	if yc.ca.Notify != nil {
		go yc.ca.Notify()
	}

	return nil
} // }}}

// func YConf.Get {{{

// Returns the currently loaded configuration.
//
// Returns nil if no value found (not yet loaded)
func (yc *YConf) Get() interface{} {
	fl := yc.l.With().Str("func", "Get").Logger()

	yc.loMut.RLock()
	lo := yc.lo
	yc.loMut.RUnlock()

	if lo == nil {
		fl.Debug().Bool("loaded", false).Send()
		return nil
	}

	fl.Debug().Bool("loaded", true).Send()
	return lo.conf
} // }}}

// func YConf.hasChanged {{{

// Returns true if there is a file in the configuration directory that is newer then the last newest.
func (yc *YConf) hasChanged(newest time.Time, path string) (bool, error) {
	fl := yc.l.With().Str("func", "hasChanged").Str("path", path).Logger()

	f, err := os.Open(path)
	if err != nil {
		fl.Err(err).Msg("open")
		return true, err
	}

	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		fl.Err(err).Msg("stat")
		return true, err
	}

	if s.ModTime().After(newest) {
		return true, nil
	}

	files, err := f.Readdir(-1)

	if err != nil {
		fl.Err(err).Msg("readdir")
		return true, err
	}

	// Now check the times of each file.

	// Lets check each file now.
	for _, file := range files {
		name := file.Name()

		if file.IsDir() {
			// Recursion.
			changed, err := yc.hasChanged(newest, filepath.Join(path, name))
			if err != nil {
				return true, err
			}

			if changed {
				return true, nil
			}
			continue
		}

		// Ensure this is a file we are interested in.
		if !file.Mode().IsRegular() || !yc.isConf(name) {
			continue
		}

		if file.ModTime().After(newest) {
			return true, nil
		}
	}

	return false, nil
} // }}}

// func YConf.CheckConf {{{

func (yc *YConf) CheckConf() error {
	fl := yc.l.With().Str("func", "CheckConf").Logger()

	// We need to get the last time we saw a modified file here.
	// So get a read lock to grab that quickly.
	yc.loMut.RLock()
	newest := yc.lo.newest
	yc.loMut.RUnlock()

	changed, err := yc.hasChanged(newest, yc.confPath)

	if err != nil {
		return err
	}

	if !changed {
		fl.Debug().Bool("changed", false).Send()
		return nil
	}

	fl.Debug().Bool("changed", true).Send()

	// Now load the configuration files.
	if err := yc.reload(); err != nil {
		fl.Err(err).Msg("reload")
		return err
	}

	return nil
} // }}}

// func YConf.loadConf {{{

func (yc *YConf) loadConf(lo *loaded, path string) error {
	fl := yc.l.With().Str("func", "loadConf").Str("path", path).Logger()

	// Attempt to open the provided path.
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		return err
	}

	// File or directory?
	if !s.IsDir() {
		// Ok, so just a file.
		// Ensure this is a file we are interested in.
		if !s.Mode().IsRegular() || !yc.isConf(path) {
			err := errors.New("Not a directory or configuration file")
			fl.Err(err).Send()
			return err
		}

		// Track our most recent file time.
		if s.ModTime().After(lo.newest) {
			lo.newest = s.ModTime()
		}

		if err := yc.loadConfFile(lo, path); err != nil {
			return err
		}

		return nil
	}

	if s.ModTime().After(lo.newest) {
		lo.newest = s.ModTime()
	}

	files, err := f.Readdir(-1)
	if err != nil {
		fl.Err(err).Msg("readdir")
		return fmt.Errorf("readdir(%s): %s", path, err)
	}

	// Sort the files, this allows you to have some files load before or after others simply by the names of the files.
	sort.Sort(fileSort(files))

	// Lets check each file now.
	for _, file := range files {
		name := file.Name()

		// Skip files starting with '.' (or basic sanity, empty names)
		if len(name) < 1 || name[0] == '.' {
			continue
		}

		// Is this a directory?
		if file.IsDir() {
			// Recursion.
			err := yc.loadConf(lo, filepath.Join(path, name))
			if err != nil {
				return err
			}
			continue
		}

		// Ensure this is a file we are interested in.
		if !file.Mode().IsRegular() || !yc.isConf(name) {
			continue
		}

		// Track our most recent file time.
		if file.ModTime().After(lo.newest) {
			lo.newest = file.ModTime()
		}

		if err := yc.loadConfFile(lo, filepath.Join(path, name)); err != nil {
			return err
		}
	}

	return nil
} // }}}

// func YConf.loadConfFile {{{

func (yc *YConf) loadConfFile(lo *loaded, file string) error {
	var err error

	fl := yc.l.With().Str("func", "loadConfFile").Str("file", file).Logger()

	f, err := os.Open(file)
	if err != nil {
		fl.Err(err).Msg("open")
		return fmt.Errorf("open(%s): %s", file, err)
	}

	ei := yc.ca.Empty()

	fl.Debug().Interface("empty", ei).Send()

	// Load the new configuration.
	if err := yaml.NewDecoder(f).Decode(ei); err != nil {
		fl.Err(err).Msg("decode")
		return fmt.Errorf("decode(%s): %s", file, err)
	}

	if yc.ca.Convert != nil {
		ei, err = yc.ca.Convert(ei)
		if err != nil {
			fl.Err(err).Msg("convert")
			return err
		}
		fl.Debug().Interface("converted", ei).Send()
	}

	// Something already loaded?
	if lo.conf != nil {
		// Is there a merge function?
		if yc.ca.Merge != nil {
			// Yep, so we need a merge.
			lo.conf, err = yc.ca.Merge(lo.conf, ei)
			if err != nil {
				fl.Err(err).Msg("merge")
				return err
			}
			fl.Debug().Interface("merged", lo.conf).Send()
		} else {
			fl.Debug().Msg("replace")
			// No merge, so just replace.
			lo.conf = ei
		}
		fl.Debug().Interface("loaded", lo.conf).Send()
	} else {
		// Nope, first load - So just set conf.
		lo.conf = ei
		fl.Debug().Interface("loaded", lo.conf).Send()
	}

	return nil
} // }}}

// func YConf.isConf {{{

func (yc *YConf) isConf(name string) bool {
	// Get the last '.' in the file to find its extension.
	dot := strings.LastIndexByte(name, '.')

	// Must have a '.' (so can not be 0), can not start with '.' (so can not be 1), and can not end in dot.
	if dot < 1 || dot == len(name) || dot > len(name)-1 {
		return false
	}

	// Grab the extension
	ext := strings.ToLower(name[dot+1:])

	switch ext {
	case "yaml":
		fallthrough
	case "json":
		return true
	}

	return false
} // }}}

// func YConf.loopy {{{

// Handles automatic checking for new or changed configuration files.
func (yc *YConf) loopy() {
	fl := yc.l.With().Str("func", "loopy").Logger()

	// Basic tracking ticker, runs every minute.
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			fl.Debug().Msg("tick")
			yc.CheckConf()
		case _, ok := <-yc.bye:
			if !ok {
				fl.Debug().Msg("Shutting down")
				return
			}
		}
	}
} // }}}

type fileSort []os.FileInfo

func (fs fileSort) Len() int           { return len(fs) }
func (fs fileSort) Less(i, j int) bool { return fs[i].Name() < fs[j].Name() }
func (fs fileSort) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }
