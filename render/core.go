package render

import (
	"context"
	"errors"
	"frame/types"
	"frame/yconf"
	"image"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &confYAML{} },
	Merge:   yconfMerge,
	Convert: yconfConvert,
	Changed: yconfChanged,
}

// func yconfMerge {{{

func yconfMerge(inAInt, inBInt interface{}) (interface{}, error) {
	// Its important to note that previouisly loaded files are passed in a inA, where as inB is just the most recent.
	// This means that for our various maps, inA will continue to grow as the number of files we process grow, but inB will always be just the
	// most recent.
	//
	// So merge everything into inA.
	inA, ok := inAInt.(*conf)
	if !ok {
		return nil, errors.New("not a *conf")
	}

	inB, ok := inBInt.(*conf)
	if !ok {
		return nil, errors.New("not a *conf")
	}

	// Merge the profiles
	if len(inA.profiles) == 0 {
		// If A has no profiles then just assign B.
		inA.profiles = inB.profiles
	} else {
		// Merge in B's profiles.
		for _, prof := range inB.profiles {
			inA.profiles = append(inA.profiles, prof)
		}
	}

	return inA, nil
} // }}}

// func yconfChanged {{{

func yconfChanged(origConfInt, newConfInt interface{}) bool {
	// None of these casts should be able to fail, but we like our sanity.
	origConf, ok := origConfInt.(*conf)
	if !ok {
		return true
	}

	newConf, ok := newConfInt.(*conf)
	if !ok {
		return true
	}

	if len(origConf.profiles) != len(newConf.profiles) {
		return true
	}

	// Both origConf and newConf.profiles are the same length, so this
	// is otherwise safe.
	for i := 0; i < len(origConf.profiles); i++ {
		if origConf.profiles[i] != newConf.profiles[i] {
			return true
		}
	}

	return false
} // }}}

// func yconfConvert {{{

func yconfConvert(inInt interface{}) (interface{}, error) {
	in, ok := inInt.(*confYAML)
	if !ok {
		return nil, errors.New("not *confYAML")
	}

	out := &conf{}

	if len(in.Profiles) < 1 {
		return nil, errors.New("file has no profiles")
	}

	for _, prof := range in.Profiles {
		op := confProfile{
			depth:         prof.MaxDepth,
			tagProfile:    prof.TagProfile,
			writeInterval: prof.WriteInterval,
			outputFile:    prof.OutputFile,
		}

		// Assign defaults.
		if op.depth < 1 || op.depth > 20 {
			op.depth = 6
		}

		if op.tagProfile == "" {
			return nil, errors.New("no TagProfile")
		}

		if op.outputFile == "" {
			return nil, errors.New("no OutputFile")
		}

		if prof.Width == 0 || prof.Height == 0 {
			return nil, errors.New("no Width or Height")
		}

		op.size = image.Point{prof.Width, prof.Height}

		// Default the writeInterval to 5 minutes (60s*5)
		if op.writeInterval < time.Second {
			op.writeInterval = time.Second * 300
		}

		// Append the profile.
		out.profiles = append(out.profiles, op)
	}

	return out, nil
} // }}}

// func New {{{

func New(confPath string, we types.Weighter, cm types.CacheManager, l *zerolog.Logger, ctx context.Context) (*Render, error) {
	var err error

	re := &Render{
		l:     l.With().Str("mod", "render").Logger(),
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
		we:    we,
		cm:    cm,
		cPath: confPath,
		ctx:   ctx,
	}

	fl := re.l.With().Str("func", "New").Logger()

	// Load our configuration.
	if err = re.loadConf(); err != nil {
		return nil, err
	}

	// Start background processing to watch configuration for changes.
	re.yc.Start()

	// Start the background goroutine that monitors the profile intervals
	// for writing out the profile images.
	go re.loopy()

	fl.Debug().Send()

	return re, nil
} // }}}

// func Render.loadConf {{{

// This is called by New() to load the configuration the first time.
func (re *Render) loadConf() error {
	var err error

	fl := re.l.With().Str("func", "loadConf").Logger()

	// Avoid us running twice (should not be possible), but more importantly, this will
	// called notifyConf() before we are ready, so this lets them know to just return.
	if !atomic.CompareAndSwapUint32(&re.start, 0, 1) {
		err := errors.New("loadConf already running")
		fl.Err(err).Send()
		return err
	}

	defer atomic.StoreUint32(&re.start, 0)

	// Copy the default ycCallers, we need to copy this so we can add our own notifications.
	ycc := ycCallers

	ycc.Notify = func() {
		re.notifyConf()
	}

	if re.yc, err = yconf.New(re.cPath, ycc, &re.l, re.ctx); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	// Run a simple once-through check, not the full Start() yet.
	if err = re.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", re.yc.Get()).Send()

	// Get the loaded configuration
	co, ok := re.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	// Check the configuration sanity first.
	if !re.checkConf(co) {
		return errors.New("Invalid configuration")
	}

	// Looks good, go ahead and store it.
	re.co.Store(co)

	return nil
} // }}}

// func Render.notifyConf {{{

func (re *Render) notifyConf() {
	fl := re.l.With().Str("func", "notifyConf").Logger()

	// If loadConf() is running then we just return right away.
	if atomic.LoadUint32(&re.start) == 1 {
		return
	}

	// Update our configuration.
	co, ok := re.yc.Get().(*conf)
	if !ok {
		fl.Warn().Msg("Get failed")
		return
	}

	// Check the new configuration before we do anything.
	if !re.checkConf(co) {
		fl.Warn().Msg("Invalid configuration, continuing to run with previously loaded configuration")
		return
	}

	// Store the new configuration
	re.co.Store(co)

	// Note - We did not check ucPollInt here, thats handled in the partial loop and it will adjust on its next patial run.
	fl.Info().Msg("configuration updated")
} // }}}

// func Render.checkConf {{{

func (re *Render) checkConf(co *conf) bool {
	fl := re.l.With().Str("func", "checkConf").Logger()

	if len(co.profiles) == 0 {
		fl.Warn().Msg("No profiles")
		return false
	}

	return true
} // }}}

// func Render.getConf {{{

func (re *Render) getConf() *conf {
	fl := re.l.With().Str("func", "getConf").Logger()

	if co, ok := re.co.Load().(*conf); ok {
		return co
	}

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing conf?")
	return &conf{}
} // }}}

// func Render.loopy {{{

// Handles our basic background tasks, partial and full queries.
func (re *Render) loopy() {
	fl := re.l.With().Str("func", "loopy").Logger()

	fl.Debug().Send()

} // }}}
