package render

import (
	"context"
	"errors"
	fimg "frame/image"
	"frame/types"
	"frame/yconf"
	"image"
	"image/draw"
	"math/rand"
	"os"
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

	if len(inA.MixProfiles) == 0 {
		inA.MixProfiles = inB.MixProfiles
	} else {
		for _, prof := range inB.MixProfiles {
			inA.MixProfiles = append(inA.MixProfiles, prof)
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

	if len(origConf.MixProfiles) != len(newConf.MixProfiles) {
		return true
	}

	for i := 0; i < len(origConf.MixProfiles); i++ {
		if origConf.MixProfiles[i] != newConf.MixProfiles[i] {
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

	if len(in.Profiles) < 1 && len(in.MixProfiles) < 1 {
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

	for _, prof := range in.MixProfiles {
		op := &confProfileMixed{
			WriteInterval: prof.WriteInterval,
			OutputFile:    prof.OutputFile,
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

		for _, pcount := range prof.Profiles {
			cp := confProfileCounts{
				TagProfile: pcount.TagProfile,
				images:     pcount.Images,
			}

			op.Profiles = append(op.Profiles, cp)
		}

		// Append the profile.
		out.MixProfiles = append(out.MixProfiles, op)
	}

	return out, nil
} // }}}

// func New {{{

func New(confPath string, we types.Weighter, cm types.CacheManager, l *zerolog.Logger, ctx context.Context) (*Render, error) {
	var err error

	re := &Render{
		l:     l.With().Str("mod", "render").Logger(),
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

	for _, prof := range co.MixProfiles {
		go re.renderProfileMixed(prof)
	}

	fl.Debug().Send()

	return re, nil
} // }}}

// func Render.loadConf {{{

// This is called by New() to load the configuration the first time.
func (re *Render) loadConf() error {
	var err error

	fl := re.l.With().Str("func", "loadConf").Logger()

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

	// Same as above, check the WeighterProfile for each MixProfile.
	for _, prof := range co.MixProfiles {
		// Note - prof.Profiles are not references, so access them differently.
		for i := 0; i < len(prof.Profiles); i++ {
			if prof.Profiles[i].wp, err = re.we.GetProfile(prof.Profiles[i].TagProfile); err != nil {
				fl.Err(err).Msg("Weighter.GetProfile")
				return false
			}
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

// func Render.renderImage {{{

// r can be null, in which case a temporary random number generator is used.
// No other value can be null.
func (re *Render) renderImage(size image.Point, file string, ids []uint64) error {
	var err error

	fl := re.l.With().Str("func", "renderImage").Str("OutputFile", file).Logger()

	// Used to determine the location of the next image.
	// Top/Left or Bottom/Right.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	start := time.Now()

	// For very new profiles this can happen that no IDs are returned.
	//
	// Or images being taken disabled/deleted that cause a profile to no longer have any.
	if len(ids) < 1 {
		err = errors.New("no IDs provided")
		fl.Err(err).Send()
		return err
	}

	// Ok, we have all the IDs we need.
	// Create a new blank image.
	img := image.NewRGBA(image.Rect(0, 0, size.X, size.Y))

	// Create our sub image.
	// This will be a smaller image within the main image, getting
	// smaller each time a portion of the main image is filled.
	sub := img

	fl.Debug().Interface("ids", ids).Msg("check")

	// Loop through all the IDs we have until we either out or have
	// too few pixels to place the image within.
	for _, id := range ids {
		sub, err = re.fillImage(sub, id, r)
		if err != nil {
			fl.Err(err).Msg("fillImage")
			return err
		}

		// If no sub is returned then we have not enough left over space on the image itself to put anymore.
		if sub == nil {
			fl.Debug().Interface("ids", ids).Uint64("id", id).Msg("no more")
			break
		}
	}

	// Now we open the file to write out the image.
	//
	// We do not defer f.Close since we want to close it right away so we can rename it.
	f, err := os.OpenFile(file+".tmp", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fl.Err(err).Msg("OpenFile")
		return err
	}

	// Encode the image.
	if err := fimg.SaveImageWebP(f, img); err != nil {
		f.Close()
		fl.Err(err).Msg("SaveImageWebP")
		return err
	}

	f.Close()

	if err := os.Rename(file+".tmp", file); err != nil {
		fl.Err(err).Msg("Rename")
		return err
	}

	// Ok, image complete.
	fl.Debug().Stringer("took", time.Since(start)).Send()

	return nil
} // }}}

// func Render.renderProfileMixed {{{

func (re *Render) renderProfileMixed(prof *confProfileMixed) {
	var ids []uint64

	fl := re.l.With().Str("func", "renderProfileMixed").Str("OutputFile", prof.OutputFile).Logger()

	// We use an atomic uint32 to let us know if we are already rendering
	// an image for this profile.
	if !atomic.CompareAndSwapUint32(&prof.running, 0, 1) {
		return
	}

	defer atomic.StoreUint32(&prof.running, 0)

	// Loop through the mixed profiles to get the IDs we want.
	for _, cpc := range prof.Profiles {
		// Lets get the image IDs we need, up to a max of Depth.
		tids, err := cpc.wp.Get(cpc.images)
		if err != nil {
			// If Weighter was shutdown, jut return.
			if errors.Is(err, types.ErrShutdown) {
				fl.Info().Msg("in shutdown")
				return
			}

			// Something went wrong, lets see if we can fix it by getting a new
			// WeighterProfile.
			cpc.wp, err = re.we.GetProfile(cpc.TagProfile)
			if err != nil {
				fl.Err(err).Msg("Weighter.GetProfile")
				return
			}

			// Ok, take 2 for getting the IDs.
			if tids, err = cpc.wp.Get(cpc.images); err != nil {
				fl.Err(err).Msg("WeighterProfile.Get")
				return
			}
		}

		ids = append(ids, tids...)
	}

	// For very new profiles this can happen that no IDs are returned.
	//
	// Or images being taken disabled/deleted that cause a profile to no longer have any.
	if len(ids) < 1 {
		fl.Warn().Msg("no images returned, nothing to render")
		return
	}

	// Now hand the details off to be rendered.
	if err := re.renderImage(prof.Size, prof.OutputFile, ids); err != nil {
		fl.Err(err).Msg("renderImage")
		return
	}
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

	// For very new profiles this can happen that no IDs are returned.
	//
	// Or images being taken disabled/deleted that cause a profile to no longer have any.
	if len(ids) < 1 {
		fl.Warn().Msg("no images returned, nothing to render")
		return
	}

	// Now hand the details off to be rendered.
	if err := re.renderImage(prof.Size, prof.OutputFile, ids); err != nil {
		fl.Err(err).Msg("renderImage")
		return
	}
} // }}}

// func Render.toRGBA {{{

func (re *Render) toRGBA(img image.Image) *image.RGBA {
	// First basic check - Is the image already a RGBA?
	if rgba, ok := img.(*image.RGBA); ok {
		// Yep, no conversion needed then.
		return rgba
	}

	// So we have to convert. Doing this all ourselves is a pain in the ass, so rather we let Go do it.
	// We create a new image.RGBA of the same size and copy the pixels to it letting Go handle all the converisions.
	//
	// Get the size of the original image.
	bnds := img.Bounds()

	// Now make a new RGBA image with that size.
	rgba := image.NewRGBA(bnds)

	// Copy the source to the destination.
	draw.Draw(rgba, bnds, img, image.Point{}, draw.Src)

	return rgba
} /// }}}

// func Render.fillImage {{{

// Provided an image and an ID, we fill the image as much as possible by resizing the ID to fit.
//
// We then return any portion of the image left that we were unable to fill.
//
// r provided is expected to be thread safe or the caller otherwise has a lock.
func (re *Render) fillImage(img *image.RGBA, id uint64, r *rand.Rand) (*image.RGBA, error) {
	var layoutFlip bool

	fl := re.l.With().Str("func", "fillImage").Logger()

	// Lets get the current image size.
	imgB := img.Bounds()
	imgS := imgB.Size()

	// Now get the resized ID image.
	tmpImg, err := re.cm.LoadImage(id, imgS, true)
	if err != nil {
		fl.Err(err).Msg("LoadImage")
		return nil, err
	}

	// Ensure its an image.RGBA, so all images are consistent.
	idImg := re.toRGBA(tmpImg)

	// Ok, we asked the image to be resized to fit at least 1 dimension (width or height) fully.
	// So unless the image is an exact fit, we expect to have some pixels available on one of
	// those dimensions.
	//
	// Lets figure out which has the most pixels?
	idB := idImg.Bounds()
	idS := idB.Size()

	// Sometimes there can be an exact match.
	//
	// Do we have one here?
	if imgS == idS {
		fl.Debug().Stringer("imgS", imgS).Stringer("idS", idS).Msg("perfect fit")

		// Perfect fit.
		draw.Draw(img, imgB, idImg, idB.Min, draw.Src)
		return nil, nil
	}

	// Do we flip the layout or not?
	//
	// Meaning, rather then the top/left, we align to bottom/right
	if r.Intn(2) > 0 {
		layoutFlip = true
	}

	// This will be adjusted to whatever area is left over after we figure out where
	// idImg fits within img.
	emptySpace := imgB

	// Where idImg will be palced within img.
	//
	// We pass this in to draw.Draw() so it can place the image properly.
	newLoc := imgB

	// Lets figure out the location within img to put idImg.
	if layoutFlip {
		// Ok, bottom/left alignment here.
		//
		// Is the width the same between the two images?
		if imgS.X == idS.X {
			// Ok, width is the same. Not an exact fit, so we expect left over space on the height.
			//
			// Since we are flipped, we set the new Min for Y (height) to be from the bottom, leaving any empty
			// space at the top.
			//
			newLoc.Min.Y = imgB.Max.Y - idS.Y

			// Remove the pixels used by the image from the empty space.
			emptySpace.Max.Y = newLoc.Min.Y
		} else {
			// Ok, going by width here.
			// Pretty much identical to above, except on X (width).
			newLoc.Min.X = imgB.Max.X - idS.X
			emptySpace.Max.X = newLoc.Min.X
		}
	} else {
		// We are not flipped.
		// So rather then placing the image on the bottom/right, we are placing it on the top/left.
		if imgS.X == idS.X {
			// We have left over height.
			newLoc.Max.Y = newLoc.Min.Y + idS.Y

			// Empty space now starts after the image above, so just set its Min to the previous MaxD.
			emptySpace.Min.Y = newLoc.Max.Y
		} else {
			// Left over width.
			//
			// Same logic as above, just for X this time.
			newLoc.Max.X = newLoc.Min.X + idS.X
			emptySpace.Min.X = newLoc.Max.X
		}
	}

	fl.Debug().Stringer("imgS", imgS).Stringer("idS", idS).Stringer("newLoc", newLoc).Stringer("emptySpace", emptySpace).Bool("layoutFlip", layoutFlip).Msg("dimensions")

	// Now copy the image inside out existing one.
	draw.Draw(img, newLoc, idImg, idImg.Bounds().Min, draw.Src)

	// If emptySpace is too small, we do not return an image.
	esS := emptySpace.Bounds().Size()
	if esS.X < 10 || esS.Y < 10 {
		return nil, nil
	}

	// emptySpace is large enough to fit something else, so get it to return.
	subImg := img.SubImage(emptySpace).(*image.RGBA)

	fl.Debug().Send()

	return subImg, nil
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

	for _, prof := range co.MixProfiles {
		// Same logic as above.
		added = false

		// Does an interval already exist for this profile to tag along on?
		for i, _ := range rInts {
			if rInts[i].WriteInt == prof.WriteInterval {
				// Same duration so just append.
				rInts[i].Mixed = append(rInts[i].Mixed, prof)

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

		ri.Mixed = append(ri.Mixed, prof)
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

	ourUpdated := atomic.LoadUint32(&re.updated)

	// Get the initial intervals
	intervals := re.makeRenderIntervals()

	// Lets change the tick to the first check we need.
	rTick.Reset(intervals[0].NextDur)

	fl.Debug().Interface("intervals", intervals).Send()

	fl.Debug().Stringer("NextDur", intervals[0].NextDur).Msg("first tick waiting")

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
			if intervals[0].Profiles != nil {
				for _, prof := range intervals[0].Profiles {
					fl.Debug().Str("file", prof.OutputFile).Msg("profileTick")
					go re.renderProfile(prof)
				}
			}

			// Mixed profiles.
			if intervals[0].Mixed != nil {
				for _, prof := range intervals[0].Mixed {
					fl.Debug().Str("file", prof.OutputFile).Msg("mixedTick")
					go re.renderProfileMixed(prof)
				}
			}

			// Update our intervals
			intervals = re.setRenderIntervals(intervals)

			// And our baseTick
			rTick.Reset(intervals[0].NextDur)

			fl.Debug().Stringer("NextDur", intervals[0].NextDur).Msg("next tick")
		case _, ok := <-ctx.Done():
			if !ok {
				return
			}
		}
	}
} // }}}
