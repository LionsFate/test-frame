package weighter

import (
	"context"
	"errors"
	"frame/tags"
	"frame/types"
	"frame/yconf"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
)

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

	if inA.Database != inB.Database && inB.Database != "" {
		inA.Database = inB.Database
	}

	if inA.Queries.Full != inB.Queries.Full && inB.Queries.Full != "" {
		inA.Queries.Full = inB.Queries.Full
	}

	if inA.Queries.Poll != inB.Queries.Poll && inB.Queries.Poll != "" {
		inA.Queries.Poll = inB.Queries.Poll
	}

	if len(inB.TagRules) > 0 && !inA.TagRules.Equal(inB.TagRules) {
		inA.TagRules = inA.TagRules.Combine(inB.TagRules)
	}

	if inA.PollInterval != inB.PollInterval && inB.PollInterval > 0 {
		inA.PollInterval = inB.PollInterval
	}

	if inA.FullInterval != inB.FullInterval && inB.FullInterval > 0 {
		inA.FullInterval = inB.FullInterval
	}

	// If A has no profiles but B does?
	// Just copy them over as-is, easy enough.
	if inA.Profiles == nil && inB.Profiles != nil {
		inA.Profiles = inB.Profiles
	} else if inA.Profiles != nil && inB.Profiles != nil {
		// Copy the profiles, this one is a little more complex.
		for kb, vb := range inB.Profiles {
			va, ok := inA.Profiles[kb]
			if !ok {
				// Value does not exist in A, so just set it.
				inA.Profiles[kb] = vb
				continue
			}

			// Value exists in both A and B, so we need to combine the weights.
			va.Weights = va.Weights.Combine(vb.Weights)
			va.Matches.Combine(&vb.Matches)
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

	if origConf.Database != newConf.Database {
		return true
	}

	if origConf.Queries.Full != newConf.Queries.Full {
		return true
	}

	if origConf.Queries.Poll != newConf.Queries.Poll {
		return true
	}

	if !origConf.TagRules.Equal(newConf.TagRules) {
		return true
	}

	if origConf.PollInterval != newConf.PollInterval {
		return true
	}

	if origConf.FullInterval != newConf.FullInterval {
		return true
	}

	if len(origConf.Profiles) != len(newConf.Profiles) {
		return true
	}

	for name, oProf := range origConf.Profiles {
		nProf, ok := newConf.Profiles[name]
		if !ok {
			return true
		}

		if !oProf.Weights.Equal(nProf.Weights) {
			return true
		}

		if !oProf.Matches.Equal(nProf.Matches) {
			return true
		}
	}

	return false
} // }}}

// func New {{{

func New(confPath string, tm types.TagManager, l *zerolog.Logger, ctx context.Context) (*Weighter, error) {
	var err error

	we := &Weighter{
		l:     l.With().Str("mod", "weighter").Logger(),
		tm:    tm,
		cPath: confPath,
		ctx:   ctx,
	}

	// Create our empty cache.
	we.ca = &cache{
		images:   make(map[uint64]*cacheImage, 0),
		profiles: make(map[string]*cacheProfile, 0),
	}

	fl := we.l.With().Str("func", "New").Logger()

	// Load our configuration.
	//
	// This also handles connecting to the database.
	if err = we.loadConf(); err != nil {
		return nil, err
	}

	// Now run the initial doFull() and ensure things are OK.
	if err := we.doFull(); err != nil {
		return nil, err
	}

	// Start background processing to watch configuration for changes.
	we.yc.Start()

	// Start the regular database background loop.
	go we.loopy()

	fl.Debug().Send()

	return we, nil
} // }}}

// func wProfile.loadCP {{{

func (wp *wProfile) loadCP() (*cacheProfile, error) {
	fl := wp.we.l.With().Str("func", "loadCP").Logger()

	// Attempt to load the existing cacheProfile
	cp, ok := wp.cp.Load().(*cacheProfile)

	// The one we have stored still good?
	if ok && atomic.LoadUint32(&cp.closed) == 0 {
		fl.Debug().Str("profile", cp.profile).Msg("loaded")
		// Perfect, return away.
		return cp, nil
	}

	// The one we have stored is invalid somehow, so lets get a new one.
	//
	// Get the cache
	ca := wp.we.ca

	// Get a lock on the cache
	ca.pMut.RLock()
	defer ca.pMut.RUnlock()

	// Does the profile exist?
	//
	// We do not check if it is closed or not here since we have
	// a read lock. It can not be closed while we have the lock.
	if cp, ok := ca.profiles[cp.profile]; ok {
		fl.Debug().Str("profile", cp.profile).Msg("found")

		// Found a newer one, so replace our stored one.
		wp.cp.Store(cp)
		return cp, nil
	}

	// No valid one can be found.
	// This can happen if a profile is valid and then the configuration
	// changes, making the profile now invalid.
	//
	// Normal part of operations and should be handled.
	//
	// As a result, we do not log your typical error here.
	err := errors.New("invalid profile")
	fl.Debug().Err(err).Send()
	return nil, err
} // }}}

// func wProfile.Get {{{

func (wp *wProfile) Get(num uint8) ([]uint64, error) {
	cp, err := wp.loadCP()
	if err != nil {
		return nil, err
	}

	// For sanity we cap the number at 100.
	if num > 100 {
		num = 100
	}

	// Sanity - Handle empty profiles.
	if cp.maxRoll == 0 {
		return nil, errors.New("no images for tagprofile")
	}

	ids := wp.we.getRandomProfile(cp, num)
	return ids, nil
} // }}}

// func Weighter.getRandomProfile {{{

func (we *Weighter) getRandomProfile(cp *cacheProfile, num uint8) []uint64 {
	fl := we.l.With().Str("func", "getRandomProfile").Str("profile", cp.profile).Uint8("num", num).Logger()

	// Mutex for accessing our random number generator.
	cp.rMut.Lock()
	defer cp.rMut.Unlock()

	fl.Debug().Int("maxRoll", cp.maxRoll).Send()

	ids := make([]uint64, num)
	for i := uint8(0); i < num; i++ {
		// Get the random weight to use.
		weight := cp.r.Intn(cp.maxRoll)

		// Find the matching weight.
		for _, wl := range cp.weights {
			// Is the weight we are looking at less then what we want?
			if wl.Weight+wl.Start < weight {
				continue
			}

			// This one matches. So lets grab a random file within.

			ids[i] = wl.IDs[cp.r.Intn(len(wl.IDs))]
			break
		}
	}

	return ids
} // }}}

// func Weighter.GetProfile {{{

func (we *Weighter) GetProfile(pr string) (types.WeighterProfile, error) {
	fl := we.l.With().Str("func", "GetProfile").Logger()

	if pr == "" {
		err := errors.New("invalid profile")
		fl.Err(err)
		return nil, err
	}

	ca := we.ca

	// Get a lock on the cache
	ca.pMut.RLock()
	defer ca.pMut.RUnlock()

	// Does the profile exist?
	//
	// We do not check if it is closed or not here since we have
	// a read lock. It can not be closed while we have the lock.
	if cp, ok := ca.profiles[pr]; ok {
		fl.Debug().Str("profile", pr).Msg("found")
		// Alright, here you go.
		wp := &wProfile{
			we: we,
		}

		// We use atomic.Value to make multiple goroutines a lot easier.
		wp.cp.Store(cp)
		return wp, nil
	}

	err := errors.New("profile not found")
	fl.Err(err)
	return nil, err
} // }}}

// func Weighter.makeProfileWeights {{{

func (we *Weighter) makeProfileWeights(ca *cache) error {
	var weight int

	fl := we.l.With().Str("func", "makeProfileWeights").Logger()

	co := we.getConf()

	// Basic sanity - No profiles, nothing we can actually do.
	if len(co.Profiles) < 1 {
		fl.Warn().Msg("No profiles")
		return errors.New("No profiles")
	}

	// We need a temporary profile map to store the weights we are figuring out.
	tpMap := make(map[string]map[int][]uint64, len(co.Profiles))

	// Create each profiles temporary weights map
	for pName, _ := range co.Profiles {
		tpMap[pName] = make(map[int][]uint64, 100)
	}

	// We tend to have far less profiles vs. images, so lets just iterate through
	// the images only 1 time, checking each profile as we go through the images.
	for id, ci := range ca.images {
		for pName, prof := range co.Profiles {
			// If it doesn't match what the profile wants, skip it.
			if !prof.Matches.Give(ci.Tags) {
				continue
			}

			// Ok, matches - What weight will it be given?
			weight = prof.Weights.GetWeight(ci.Tags)
			if weight < 1 {
				// A negative weight means skip it.
				continue
			}

			// Ok, we have a positive weight, so go ahead and add this image to tpMap
			tpMap[pName][weight] = append(tpMap[pName][weight], id)
		}
	}

	// Ok, so now we are setting the profiles in cache.
	// We need the lock for this.
	ca.pMut.Lock()
	defer ca.pMut.Unlock()

	// The existing profiles map, as we are going to just
	// create a new one here, but we need to invalidate the old ones
	// after the new ones are ready.
	oldProfiles := ca.profiles

	// Create the new profiles map.
	ca.profiles = make(map[string]*cacheProfile, len(tpMap))

	// Go through each profile with at least 1 image in tpMap and add it properly to the cache.
	for pName, weightMap := range tpMap {
		start := 0
		ncp := &cacheProfile{
			profile: pName,

			// Used in getRandomProfile().
			r: rand.New(rand.NewSource(time.Now().UnixNano())),
		}

		ncp.weights = make([]*weightList, 0, len(weightMap))

		// Now run through the weights.
		for weight, ids := range weightMap {
			wl := &weightList{
				Weight: weight,
				Start:  start,
				IDs:    ids,
			}

			ncp.weights = append(ncp.weights, wl)

			// The starting weight for the next
			start += weight

			// Adjust the maximum weight to roll
			ncp.maxRoll = start
		}

		// Cache the new profile.
		ca.profiles[pName] = ncp
	}

	// We have a lock on the profiles map, however any WeighterProfile
	// we have given out via Weighter.Get() has a pointer to the individual
	// cacheProfiles.
	//
	// We need to invalidate those, so they will lookup the new cacheProfile
	// from the map we are updating here.
	//
	// Loop through the old ones here and invalidate all of them now that the
	// new ones are all ready.
	for _, oldProf := range oldProfiles {
		atomic.StoreUint32(&oldProf.closed, 1)
	}

	fl.Debug().Send()

	return nil
} // }}}

// func Weighter.makeWhitelist {{{

// Makes Weighter.white, a list of all tags that we care about for filtering out images
// that can never show up so can be dropped from being tracked.
func (we *Weighter) makeWhitelist() {
	fl := we.l.With().Str("func", "makeWhitelist").Logger()

	fl.Debug().Send()

	// Get our new configuration.
	co := we.getConf()

	// A temporary map to handle duplicate issues for us.
	tmap := make(map[uint64]int, 1)

	// Iterate the profiles.
	for _, prof := range co.Profiles {
		// We only care about the weights - As it needs a positive weight to be able to be displayed.
		for _, tw := range prof.Weights {
			tmap[tw.Tag] = 1
		}
	}

	// We now have a unique list of all the tags we care about, so create the new tags.Tags for it.
	//
	// We make the capacity the length so we an just append and not worry about the index we are at.
	tgs := make(tags.Tags, 0, len(tmap))

	for k, _ := range tmap {
		tgs = append(tgs, k)
	}

	// This handles sorting for us.
	tgs = tgs.Fix()

	// And now we set the whitelist, replacing any previously existing one.
	we.white.Store(tgs)
} // }}}

// func Weighter.doFull {{{

// This does a full query as well as regenerates all the profiles.
//
// This is done at startup, periodically if configured to do so, as well as in the event of changes to the profiles.
func (we *Weighter) doFull() error {
	// Get the cache
	ca := we.ca

	// We need a write lock on the images map.
	//
	// Note that the images map is only used by queries and when generating profiles, not when asking for profile matches.
	// So its safe to aquire this lock without worry about us stalling the Weighter.
	ca.imgMut.Lock()
	defer ca.imgMut.Unlock()

	// First is the full query.
	if err := we.fullQuery(ca); err != nil {
		return err
	}

	// Now generate the profiles from all the images loaded.
	if err := we.makeProfileWeights(ca); err != nil {
		return err
	}

	return nil
} // }}}

// func Weighter.doPoll {{{

func (we *Weighter) doPoll() error {
	// Get the cache
	ca := we.ca

	// We need a write lock on the images map.
	//
	// Note that the images map is only used by queries and when generating profiles, not when asking for profile matches.
	// So its safe to aquire this lock without worry about us stalling the Weighter.
	ca.imgMut.Lock()
	defer ca.imgMut.Unlock()

	// First is the full query.
	changed, err := we.pollQuery(ca)
	if err != nil {
		return err
	}

	// Any actual changes? No changes, no updating profiles.
	if changed {
		// Now generate the profiles from all the images loaded.
		if err := we.makeProfileWeights(ca); err != nil {
			return err
		}
	}

	return nil
} // }}}

// func Weighter.pollQuery {{{

func (we *Weighter) pollQuery(ca *cache) (bool, error) {
	var id uint64
	var enabled, changed bool
	var tgs tags.Tags

	fl := we.l.With().Str("func", "pollQuery").Logger()

	// Get the whitelist to filter out images we don't care about.
	wl := we.getWhite()

	db, err := we.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return changed, err
	}

	// The query should already be prepared at connection.
	pollRows, err := db.Query(we.ctx, "poll")
	if err != nil {
		fl.Err(err).Msg("poll")
		return changed, err
	}

	for pollRows.Next() {
		// SELECT hid, tags, enabled FROM files.merged WHERE updated >= NOW() - interval '5 minutes'
		if err := pollRows.Scan(&id, &tgs, &enabled); err != nil {
			pollRows.Close()
			fl.Err(err).Msg("poll-rows-scan")
			return changed, err
		}

		// Don't assume the database doesn't have duplicates and is sorted properly.
		tgs = tgs.Fix()

		// This image already exist?
		img, ok := ca.images[id]
		if !ok {
			// Nope - Is it enabled?
			//
			// New file that is already disabled? Go ahead and skip it.
			if !enabled {
				continue
			}

			// Does it pass the whitelist?
			if !tgs.Contains(wl) {
				continue
			}

			// First file for this ID, go ahead and create it.
			img = &cacheImage{
				ID:   id,
				Tags: tgs,
			}

			changed = true
			ca.images[id] = img
			continue
		}

		// Should the file be removed?
		if !enabled {
			// Yep, so delete it and move on.
			delete(ca.images, id)
			changed = true
			continue
		}

		// Tags change?
		if !tgs.Equal(img.Tags) {
			img.Tags = tgs
			changed = true
		}
	}

	pollRows.Close()

	return changed, nil
} // }}}

// func Weighter.fullQuery {{{

func (we *Weighter) fullQuery(ca *cache) error {
	var first bool
	var id, skipped uint64
	var tgs tags.Tags

	fl := we.l.With().Str("func", "fullQuery").Logger()

	// Get the whitelist to filter out images we don't care about.
	wl := we.getWhite()

	db, err := we.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// Change seen
	ca.seen += 1

	// Is this the first run?
	//
	// Easy way to tell - there are no images.
	if len(ca.images) == 0 {
		first = true
	}

	// The query should already be prepared at connection.
	fullRows, err := db.Query(we.ctx, "full")
	if err != nil {
		fl.Err(err).Msg("full")
		return err
	}

	for fullRows.Next() {
		// SELECT hid, tags FROM files.merged WHERE enabled AND NOT blocked
		if err := fullRows.Scan(&id, &tgs); err != nil {
			fullRows.Close()
			fl.Err(err).Msg("full-rows-scan")
			return err
		}

		// Don't assume the database doesn't have duplicates and is sorted properly.
		tgs = tgs.Fix()

		// Does this contain at least 1 tag that we care about?
		if !tgs.Contains(wl) {
			skipped++
			// Nope, skip this image.
			continue
		}

		// Does this image already exist?
		img, ok := ca.images[id]
		if !ok {
			// Nope, first one - Go ahead and create it.
			img = &cacheImage{
				ID:   id,
				Tags: tgs,
				seen: ca.seen,
			}

			ca.images[id] = img

			// Image was new, added and marked as changed.
			continue
		}

		// Update seen
		img.seen = ca.seen

		// Tags change?
		if !tgs.Equal(img.Tags) {
			img.Tags = tgs
		}
	}

	fullRows.Close()

	// If its the first run then no more work to do.
	if first {
		return nil
	}

	// Now iterate images and remove any unseen.
	for _, img := range ca.images {
		if img.seen == ca.seen {
			continue
		}

		fl.Debug().Uint64("unseen", img.ID).Send()
		delete(ca.images, img.ID)
	}

	fl.Debug().Send()

	return nil
} // }}}

// func Weighter.loadConf {{{

// This is called by New() to load the configuration the first time.
func (we *Weighter) loadConf() error {
	var err error

	fl := we.l.With().Str("func", "loadConf").Logger()

	// Copy the default ycCallers, we need to copy this so we can add our own notifications.
	ycc := ycCallers

	ycc.Notify = func() {
		we.notifyConf()
	}

	ycc.Convert = func(in interface{}) (interface{}, error) {
		return we.yconfConvert(in)
	}

	if we.yc, err = yconf.New(we.cPath, ycc, &we.l, we.ctx); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	// Run a simple once-through check, not the full Start() yet.
	if err = we.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	// Get the loaded configuration
	co, ok := we.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	fl.Debug().Interface("conf", co).Send()

	// Check the configuration sanity first.
	if good, _ := we.checkConf(co, false); !good {
		return errors.New("Invalid configuration")
	}

	// Yep, so go ahead and create a new connection and get it prepared to replace the existing one.
	if err := we.dbConnect(co); err != nil {
		fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
		return err
	}

	// Looks good, go ahead and store it.
	we.co.Store(co)

	// Create the new Whitelist of tags.
	we.makeWhitelist()

	return nil
} // }}}

// func Weighter.notifyConf {{{

func (we *Weighter) notifyConf() {
	fl := we.l.With().Str("func", "notifyConf").Logger()

	// Update our configuration.
	co, ok := we.yc.Get().(*conf)
	if !ok {
		fl.Warn().Msg("Get failed")
		return
	}

	// Check the new configuration before we do anything.
	good, ucBits := we.checkConf(co, true)

	if !good {
		fl.Warn().Msg("Invalid configuration, continuing to run with previously loaded configuration")
		return
	}

	// Database change at all?
	//
	// Even if only the queries change, we do a reconnect.
	//
	// Since all our queries are prepared at connection time, this any issues having to rebind them.
	if ucBits&(ucDBConn|ucDBQuery) != 0 {
		if err := we.dbConnect(co); err != nil {
			fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
			return
		}
	}

	// The whitelist is based off the tags in the profiles.
	// So if any of the profiles changed then we need to regenerate the whitelist.
	if ucBits&ucProfiles != 0 {
		// Create the new Whitelist of tags.
		we.makeWhitelist()
	}

	// Store the new configuration
	we.co.Store(co)

	// Did anything change that would cause a full to be needed?
	//
	// Note that we include changing any queries or reconnecting as needing a full.
	//
	// This has the side benefit of allowing us at runtime to connect to a new empty database and just carry
	// on without issue.
	//
	// Obviously changing any of the TagRules or BlockTags would force another full, as skipping a full on these would
	// mean only updated images would apply these new rules.
	if ucBits&(ucDBConn|ucDBQuery|ucTagRules|ucProfiles) != 0 {
		// Something changed that should force a full
		go we.doFull()
	}

	// Note - We did not check ucPollInt here, thats handled in the partial loop and it will adjust on its next patial run.
	fl.Info().Msg("configuration updated")
} // }}}

// func Weighter.yconfConvert {{{

func (we *Weighter) yconfConvert(inInt interface{}) (interface{}, error) {
	var err error

	fl := we.l.With().Str("func", "yconfConvert").Logger()
	fl.Debug().Send()

	in, ok := inInt.(*confYAML)
	if !ok {
		return nil, errors.New("not *confYAML")
	}

	fl.Debug().Interface("yaml", in).Send()

	out := &conf{
		// No conversion needed here.
		Database: in.Database,
	}

	// We use the same structure between both, so just copy.
	out.Queries = in.Queries

	// TagRules
	if len(in.TagRules) > 0 {
		if out.TagRules, err = tags.ConfMakeTagRules(in.TagRules, we.tm); err != nil {
			return nil, err
		}
	}

	// Make the Profiles map if we need it.
	if len(in.Profiles) > 0 {
		out.Profiles = make(map[string]*confProfile, len(in.Profiles))
	}

	// The profiles.
	for name, cProf := range in.Profiles {
		// The Any, All and None we want to convert into a TagRule with the "Tag" given being the profile name.
		// Note that we will never actually assign this tag, just used for matching.
		ctr := tags.ConfTagRule{
			// The name doesn't matter since we never use this to assign any tags, so we just call it "nat" (or Not A Tag).
			// This way each profile doesn't end up being a new tag name in TagManager.
			Tag:  "nat",
			Any:  cProf.Any,
			All:  cProf.All,
			None: cProf.None,
		}

		tr, err := tags.ConfMakeTagRule(&ctr, we.tm)
		if err != nil {
			return nil, err
		}

		cp := &confProfile{
			Matches: tr,
			Name:    name,
		}

		if len(cProf.Weights) > 0 {
			cp.Weights, err = tags.ConfMakeTagWeights(cProf.Weights, we.tm)
			if err != nil {
				return nil, err
			}
		}

		// Add the new confProfile to our Profiles.
		out.Profiles[name] = cp
	}

	// The various intervals.
	if in.PollInterval > 0 {
		// Some basic sanity, force at least 1 second.
		if in.PollInterval < time.Second {
			return nil, errors.New("PollInterval too short")
		}

		out.PollInterval = in.PollInterval
	}

	if in.FullInterval > 0 {
		// Some basic sanity, force at least 1 minute.
		if in.FullInterval < time.Minute {
			return nil, errors.New("FullInterval too short")
		}

		out.FullInterval = in.FullInterval
	}

	return out, nil
} // }}}

// func Weighter.checkConf {{{

func (we *Weighter) checkConf(co *conf, reload bool) (bool, uint64) {
	var ucBits uint64

	fl := we.l.With().Str("func", "checkConf").Bool("reload", reload).Logger()

	if co.Database == "" {
		fl.Warn().Msg("Missing database")
		return false, 0
	}

	if co.Queries.Full == "" {
		fl.Warn().Msg("Missing queries.Full")
		return false, 0
	}

	if co.Queries.Poll == "" {
		fl.Warn().Msg("Missing queries.Poll")
		return false, 0
	}

	if co.PollInterval < time.Second {
		fl.Warn().Msg("PollInterval missing or too short")
		return false, 0
	}

	if co.FullInterval < time.Second {
		fl.Warn().Msg("FullInterval missing or too short")
		return false, 0
	}

	if len(co.Profiles) < 1 {
		fl.Warn().Msg("Need at least 1 profile")
		return false, 0
	}

	for _, prof := range co.Profiles {
		if len(prof.Weights) < 1 {
			fl.Warn().Msg("Profile needs at least 1 weight")
			return false, 0
		}
	}

	// If this isn't a reload, then nothing further to do.
	if !reload {
		// Basically everything changed.
		return true, ucDBConn | ucDBQuery | ucTagRules | ucProfiles | ucPollInt | ucFullInt
	}

	// Get the old configuration to compare against and figure out what changed.
	oldco := we.getConf()

	if co.Database != oldco.Database {
		ucBits |= ucDBConn
	}

	if co.Queries.Full != oldco.Queries.Full {
		ucBits |= ucDBQuery
	}

	if co.Queries.Poll != oldco.Queries.Poll {
		ucBits |= ucDBQuery
	}

	if !co.TagRules.Equal(oldco.TagRules) {
		ucBits |= ucTagRules
	}

	if co.PollInterval != oldco.PollInterval {
		ucBits |= ucPollInt
	}

	if co.FullInterval != oldco.FullInterval {
		ucBits |= ucFullInt
	}

	// Profile bits, these are a bit more involved but not horribly complex.
	if len(co.Profiles) != len(oldco.Profiles) {
		// Simple - The two have a different number of profiles.
		ucBits |= ucProfiles
	} else {
		// Same number of profiles, so run through each and see if there is a difference.
		for name, oProf := range co.Profiles {
			nProf, ok := oldco.Profiles[name]
			if !ok {
				ucBits |= ucProfiles
				break
			}

			if !oProf.Weights.Equal(nProf.Weights) {
				ucBits |= ucProfiles
				break
			}

			if !oProf.Matches.Equal(nProf.Matches) {
				ucBits |= ucProfiles
				break
			}
		}
	}

	return true, ucBits
} // }}}

// func Weighter.dbConnect {{{

func (we *Weighter) dbConnect(co *conf) error {
	var err error
	var db *pgxpool.Pool

	poolConf, err := pgxpool.ParseConfig(co.Database)
	if err != nil {
		return err
	}

	// Set the log level properly.
	cc := poolConf.ConnConfig
	cc.LogLevel = pgx.LogLevelInfo
	cc.Logger = zerologadapter.NewLogger(we.l)

	queries := &co.Queries

	// So that each connection creates our prepared statements.
	poolConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := we.setupDB(queries, conn); err != nil {
			return err
		}

		return nil
	}

	if db, err = pgxpool.ConnectConfig(we.ctx, poolConf); err != nil {
		return err
	}

	// Get the old DB (if it exists, first time it won't be set).
	oldDB, ok := we.db.Load().(*pgxpool.Pool)

	// Set the new DB (especially before we close the possible old connection)
	we.db.Store(db)

	// Close the old DB if it was set, now that the new one has replaced it.
	if ok {
		// We do this in the background, as anyone who is using it will block the Close() from returning.
		go oldDB.Close()
	}

	return nil
} // }}}

// func Weighter.setupDB {{{

// This creates all prepared statements, and if everything goes OK replaces we.db with this provided db.
func (we *Weighter) setupDB(qu *confQueries, db *pgx.Conn) error {
	fl := we.l.With().Str("func", "setupDB").Logger()

	// No using the database after a shutdown.
	if atomic.LoadUint32(&we.closed) == 1 {
		fl.Debug().Msg("called after shutdown")
		return types.ErrShutdown
	}

	// Lets prepare all our statements
	if _, err := db.Prepare(we.ctx, "full", qu.Full); err != nil {
		fl.Err(err).Msg("full")
		return err
	}

	if _, err := db.Prepare(we.ctx, "poll", qu.Poll); err != nil {
		fl.Err(err).Msg("poll")
		return err
	}

	fl.Debug().Msg("prepared")

	return nil
} // }}}

// func Weighter.getDB {{{

// Returns the current database pool.
//
// Loads it from an atomic value so that it can be replaced while running without causing issues.
func (we *Weighter) getDB() (*pgxpool.Pool, error) {
	fl := we.l.With().Str("func", "getDB").Logger()

	db, ok := we.db.Load().(*pgxpool.Pool)
	if !ok {
		err := errors.New("Not a pool")
		fl.Warn().Err(err).Send()
		return nil, err
	}

	return db, nil
} // }}}

// func Weighter.getConf {{{

func (we *Weighter) getConf() *conf {
	fl := we.l.With().Str("func", "getConf").Logger()

	if co, ok := we.co.Load().(*conf); ok {
		return co
	}

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing conf?")
	return &conf{}
} // }}}

// func Weighter.getWhite {{{

func (we *Weighter) getWhite() tags.Tags {
	fl := we.l.With().Str("func", "getWhite").Logger()

	if wl, ok := we.white.Load().(tags.Tags); ok {
		return wl
	}

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing whitelist?")
	return tags.Tags{}
} // }}}

// func Weighter.loopy {{{

// Handles our basic background tasks, partial and full queries.
func (we *Weighter) loopy() {
	var errors uint32 = 0

	fl := we.l.With().Str("func", "loopy").Logger()

	// We need to know how often we poll.
	co := we.getConf()

	ctx := we.ctx

	// Save the current PollInterval so we know if it changes.
	pollInt := co.PollInterval
	fullInt := co.FullInterval

	nextPoll := time.NewTicker(pollInt)
	nextFull := time.NewTicker(fullInt)

	defer func() {
		nextPoll.Stop()
		nextFull.Stop()
	}()

	for {
		select {
		case _, ok := <-ctx.Done():
			if !ok {
				we.close()
				return
			}
		case <-nextPoll.C:
			// Get the configuration and check if PollInterval changed
			co = we.getConf()

			if co.PollInterval != pollInt {
				// It changed, so reset the ticker.
				fl.Info().Msg("Updated PollInterval")
				pollInt = co.PollInterval
				nextPoll.Reset(pollInt)
			}

			// Run a pull.
			if err := we.doPoll(); err != nil {
				fl.Err(err).Msg("doPoll")

				// If we get a poll error, we back off on how frequently we run for sanity of those hopefully
				// trying to fix the problem.
				errors += 1

				// Update the ticker to add the errors.
				nextPoll.Reset(pollInt * time.Duration(time.Second*time.Duration(errors)))
			} else {
				// No error, so reset any possible error count.
				if errors > 0 {
					nextPoll.Reset(pollInt)
					errors = 0
				}
			}
		case <-nextFull.C:
			// Get the configuration and check if PollInterval changed
			co = we.getConf()

			if co.FullInterval != fullInt {
				// It changed, so reset the ticker.
				fl.Info().Msg("Updated FullInterval")
				fullInt = co.FullInterval
				nextFull.Reset(fullInt)
			}

			// Run a full.
			if err := we.doFull(); err != nil {
				fl.Err(err).Msg("doFull")
			}
		}
	}
} // }}}

// func Weighter.close {{{

// Stops all background processing and disconnects from the database.
func (we *Weighter) close() {
	fl := we.l.With().Str("func", "close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&we.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	if db, err := we.getDB(); err == nil {
		db.Close()
	}

	fl.Info().Msg("closed")
} // }}}
