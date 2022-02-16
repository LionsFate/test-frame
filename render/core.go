package render

import (
	"context"
	"errors"
	"frame/types"
	"frame/yconf"
	"image"
	"math/rand"
	"sort"
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
	if len(inA.Profiles) == 0 {
		// If A has no profiles then just assign B.
		inA.Profiles = inB.Profiles
	} else {
		// Merge in B's profiles.
		for _, prof := range inB.Profiles {
			inA.Profiles = append(inA.Profiles, prof)
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

	if len(origConf.Profiles) != len(newConf.Profiles) {
		return true
	}

	// Both origConf and newConf.Profiles are the same length, so this
	// is otherwise safe.
	for i := 0; i < len(origConf.Profiles); i++ {
		if origConf.Profiles[i] != newConf.Profiles[i] {
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
		op := &confProfile{
			Depth:         prof.MaxDepth,
			TagProfile:    prof.TagProfile,
			WriteInterval: prof.WriteInterval,
			OutputFile:    prof.OutputFile,
		}

		// Assign defaults.
		if op.Depth < 1 || op.Depth > 20 {
			op.Depth = 6
		}

		if op.TagProfile == "" {
			return nil, errors.New("no TagProfile")
		}

		if op.OutputFile == "" {
			return nil, errors.New("no OutputFile")
		}

		if prof.Width == 0 || prof.Height == 0 {
			return nil, errors.New("no Width or Height")
		}

		op.Size = image.Point{prof.Width, prof.Height}

		// Default the writeInterval to 5 minutes (60s*5)
		if op.WriteInterval < time.Second {
			op.WriteInterval = time.Second * 300
		}

		// Append the profile.
		out.Profiles = append(out.Profiles, op)
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

	// We start by rendering an image for each profile.
	co := re.getConf()
	for _, prof := range co.Profiles {
		go re.renderProfile(prof)
	}

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
	//
	// Note we do not update re.updated here as this is the first load.
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

	// So loopy() knows the configuration changed.
	atomic.AddUint32(&re.updated, 1)

	// Note - We did not check ucPollInt here, thats handled in the partial loop and it will adjust on its next patial run.
	fl.Info().Msg("configuration updated")
} // }}}

// func Render.checkConf {{{

func (re *Render) checkConf(co *conf) bool {
	var err error

	fl := re.l.With().Str("func", "checkConf").Logger()

	if len(co.Profiles) < 1 {
		fl.Warn().Msg("No profiles")
		return false
	}

	// Each profile we have configured must have a proper WeighterProfile
	// for it as well.
	for _, prof := range co.Profiles {
		if prof.wp, err = re.we.GetProfile(prof.TagProfile); err != nil {
			fl.Err(err).Msg("Weighter.GetProfile")
			return false
		}
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

// func Render.renderProfile {{{

func (re *Render) renderProfile(prof *confProfile) {
	fl := re.l.With().Str("func", "renderProfile").Str("OutputFile", prof.OutputFile).Logger()

	// We use an atomic uint32 to let us know if we are already rendering
	// an image for this profile.
	if !atomic.CompareAndSwapUint32(&prof.running, 0, 1) {
		return
	}

	defer atomic.StoreUint32(&prof.running, 0)

	start := time.Now()

	// Lets get the image IDs we need, up to a max of Depth.
	ids, err := prof.wp.Get(prof.Depth)
	if err != nil {
		// If Weighter was shutdown, jut return.
		if errors.Is(err, types.ErrShutdown) {
			fl.Info().Msg("in shutdown")
			return
		}

		// Something went wrong, lets see if we can fix it by getting a new
		// WeighterProfile.
		prof.wp, err = re.we.GetProfile(prof.TagProfile)
		if err != nil {
			fl.Err(err).Msg("Weighter.GetProfile")
			return
		}

		// Ok, take 2 for getting the IDs.
		if ids, err = prof.wp.Get(prof.Depth); err != nil {
			fl.Err(err).Msg("WeighterProfile.Get")
			return
		}
	}

	// Ok, we have all the IDs we need.
	// Create a new blank image.
	img := image.NewRGBA(image.Rect(0, 0, prof.Size.X, prof.Size.Y))

	// Create our sub image.
	// This will be a smaller image within the main image, getting
	// smaller each time a portion of the main image is filled.
	sub := img

	// Loop through all the IDs we have until we either out or have
	// too few pixels to place the image within.
	for _, id := range ids {
		sub, err = re.fillImage(sub, id)
		if err != nil {
			fl.Err(err).Msg("fillImage")
			return
		}
	}

	fl.Debug().Stringer("took", time.Since(start)).Send()
} // }}}

// func Render.fillImage {{{

func (re *Render) fillImage(img *image.RGBA, id uint64) (*image.RGBA, error) {
	fl := re.l.With().Str("func", "fillImage").Logger()

	fl.Debug().Send()

	return nil, errors.New("Unfinished")
} // }}}

// func Render.makeRenderIntervals {{{

func (re *Render) makeRenderIntervals() []renderInterval {
	var added bool

	fl := re.l.With().Str("func", "makeRenderIntervals").Logger()
	now := time.Now()

	co := re.getConf()

	// Create our array of intervals
	rInts := make([]renderInterval, 0, len(co.Profiles))

	for _, prof := range co.Profiles {
		// As we are multiple loops deep when adding profiles, this lets
		// us know if one was added so we can continue at a higher loop.
		added = false

		// Does an interval already exist for this profile to tag along on?
		for i, _ := range rInts {
			if rInts[i].WriteInt == prof.WriteInterval {
				// Same duration so just append.
				rInts[i].Profiles = append(rInts[i].Profiles, prof)

				// Let the lower for loop know to continue.
				added = true
				break
			}
		}

		if added {
			continue
		}

		// No existing duration match, so create a new one and add it.
		ri := renderInterval{
			WriteInt: prof.WriteInterval,
		}

		ri.Profiles = append(ri.Profiles, prof)
		rInts = append(rInts, ri)
	}

	// Now set the initial times.
	for i, _ := range rInts {
		rInts[i].NextRun = now.Add(rInts[i].WriteInt)
		rInts[i].NextDur = rInts[i].NextRun.Sub(now)
	}

	sort.Slice(rInts, func(i, j int) bool {
		return rInts[i].NextDur < rInts[j].NextDur
	})

	fl.Debug().Msg("created")

	return rInts
} // }}}

// func Render.setRenderIntervals {{{

func (re *Render) setRenderIntervals(rInts []renderInterval) []renderInterval {
	fl := re.l.With().Str("func", "setRenderIntervals").Logger()
	now := time.Now()

	// Only the first one should ever need to be updated
	if now.After(rInts[0].NextRun) {
		rInts[0].NextRun = now.Add(rInts[0].WriteInt)
		rInts[0].NextDur = rInts[0].NextRun.Sub(now)
	}

	// Now we checked the first above, but it is very possible for two profiles
	// needing to fire at the same time.
	//
	// Think of a situation where one is every 5 minutes, and another is every 2 minutes.
	// When the 10 minute check should run they both need to run, so we handle that here.
	for i, _ := range rInts {
		if now.After(rInts[i].NextRun) {
			// Looks like this one could have been skipped.
			//
			// So we update it to basically fire right away.
			rInts[i].NextRun = now.Add(time.Millisecond)
			rInts[i].NextDur = time.Millisecond
			continue
		}

		// It hasn't run yet, but just update its duration, as that keeps shrinking
		rInts[i].NextDur = rInts[i].NextRun.Sub(now)
	}

	sort.Slice(rInts, func(i, j int) bool {
		return rInts[i].NextDur < rInts[j].NextDur
	})

	fl.Debug().Send()

	return rInts
} // }}}

// func Render.loopy {{{

// Handles our basic background tasks, partial and full queries.
func (re *Render) loopy() {
	fl := re.l.With().Str("func", "loopy").Logger()

	// Default the render tick to every 5 minutes.
	rTick := time.NewTicker(5 * time.Minute)
	defer rTick.Stop()

	ctx := re.ctx

	// So we know when the configuration is updated.
	ourUpdated := atomic.LoadUint32(&re.updated)

	// Get the initial intervals
	intervals := re.makeRenderIntervals()

	// Lets change the tick to the first check we need.
	rTick.Reset(intervals[0].NextDur)

	fl.Debug().Interface("intervals", intervals).Send()

	fl.Debug().Str("OutputFile", intervals[0].Profiles[0].OutputFile).Stringer("NextDur", intervals[0].NextDur).Msg("first tick waiting")

	for {
		select {
		case <-rTick.C:
			// Did the configuration change?
			if ourUpdated != atomic.LoadUint32(&re.updated) {
				// Ok, configuration changed so we need to change the render tick
				// intervals.
				ourUpdated = atomic.LoadUint32(&re.updated)

				// Update intervals
				intervals = re.makeRenderIntervals()

				// Update the tick.
				rTick.Reset(intervals[0].NextDur)

				continue
			}

			// Run through the profiles for this interval.
			for _, prof := range intervals[0].Profiles {
				fl.Debug().Str("file", prof.OutputFile).Msg("profileTick")
				go re.renderProfile(prof)
			}

			// Update our intervals
			intervals = re.setRenderIntervals(intervals)

			// And our baseTick
			rTick.Reset(intervals[0].NextDur)

			fl.Debug().Str("OutputFile", intervals[0].Profiles[0].OutputFile).Stringer("NextDur", intervals[0].NextDur).Msg("next tick")
		case _, ok := <-ctx.Done():
			if !ok {
				return
			}
		}
	}

} // }}}
