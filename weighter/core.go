package weighter

import (
	"context"
	"errors"
	"frame/tags"
	"frame/types"
	"frame/yconf"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"sort"
	"sync/atomic"
	"time"
)

var bg = context.Background()

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

	// Copy the profiles, this one is a little more complex.
	if len(inB.Profiles) > 0 {
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

	return false
} // }}}

// func New {{{

func New(confPath string, tm types.TagManager, l *zerolog.Logger) (*Weighter, error) {
	var err error

	we := &Weighter{
		l:     l.With().Str("mod", "weighter").Logger(),
		tm:    tm,
		cPath: confPath,
		bye:   make(chan struct{}, 0),

		// Do not create the hashes, we only add ca here for the mutex.
		// The hashes is created in doFull()
		ca: &cache{},
	}

	fl := we.l.With().Str("func", "New").Logger()

	// Load our configuration.
	//
	// This also handles connecting to the database.
	if err = we.loadConf(); err != nil {
		return nil, err
	}

	fl.Debug().Send()

	// XXX BLAH BLAH startup stuff blah blah XXX

	// Start background processing to watch configuration for changes.
	we.yc.Start()

	// Start the partial loop.
	go we.loopy()

	fl.Debug().Send()

	return we, nil
} // }}}

// func Weighter.doPoll {{{

func (we *Weighter) doPoll() error {
	// First is the poll query.
	if err := we.pollQuery(); err != nil {
		return err
	}

	return nil

} // }}}

// func Weighter.makeProfileWeights {{{

func (we *Weighter) makeProfileWeights() error {
	fl := we.l.With().Str("func", "makeProfileWeights").Logger()

	fl.Debug().Send()

	return nil
} // }}}

// func Weighter.doFull {{{

// This does a full query as well as regenerates all the profiles.
//
// This is done at startup, periodically if configured to do so, as well as in the event of changes to the profiles.
func (we *Weighter) doFull() error {
	// First is the full query.
	if err := we.fullQuery(); err != nil {
		return err
	}

	// Now generate the profiles from all the images loaded.
	if err := we.makeProfileWeights(); err != nil {
		return err
	}

	return nil
} // }}}

// func Weighter.pollQuery {{{

func (we *Weighter) pollQuery() error {
	var id uint64
	var hash string
	var changed []uint64
	var enabled bool
	var tgs tags.Tags

	fl := we.l.With().Str("func", "pollQuery").Logger()

	db, err := we.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// The query should already be prepared at connection.
	pollRows, err := db.Query(bg, "poll")
	if err != nil {
		fl.Err(err).Msg("poll")
		return err
	}

	ca := we.ca

	ca.imgMut.Lock()
	defer ca.imgMut.Unlock()

	for pollRows.Next() {
		// SELECT fid, hash, tags, enabled FROM files.files WHERE updated >= NOW() - interval '5 minutes'
		//
		// I took some time to think about how I wanted to do this query.
		// Initially I wanted to pass in the most recent updated timestamp from the full query, and just get the changes since then.
		// But for this specific use case, I found that to be inefficent for the needs of the application.
		//
		// I've done things like this previously, one database would normally get thousands of rows updated every minute, so it was logical
		// to only get new updates since the last updated row seen based off that updated time.
		//
		// But this application? At least for my purposes I can see going hours, days or more without any updates.
		// So to always be asking for rows that could be from days ago?
		//
		// So I opted to move the update tracking to the query itself, and only get recently changed rows based off
		// the current time.
		if err := pollRows.Scan(&id, &hash, &tgs, &enabled); err != nil {
			pollRows.Close()
			fl.Err(err).Msg("poll-rows-scan")
			return err
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

			// First file for this hash, go ahead and create it.
			img = &cacheImage{
				ID:      id,
				Hash:    hash,
				Tags:    tgs,
			}

			changed = append(changed, id)
			ca.images[id] = img
		}

		// Should the file be removed?
		if !enabled {
			// Yep, so delete it and move on.
			delete(ca.images, id)
			changed = append(changed, id)
			continue
		}

		// Tags change?
		if !tgs.Equal(img.Tags) {
			img.Tags = tgs
			changed = append(changed, id)
		}
	}

	pollRows.Close()

	// Sort changed before we set it.
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	
	// Set the new changed.
	ca.pollChanged = changed

	return nil
} // }}}

// func Weighter.fullQuery {{{

func (we *Weighter) fullQuery() error {
	var first bool
	var id uint64
	var hash string
	var tgs tags.Tags

	fl := we.l.With().Str("func", "fullQuery").Logger()

	db, err := we.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}
	// Get the cache
	ca := we.ca

	// We need a write lock on the images map.
	//
	// Note that the images map is only used by queries and when generating profiles, not when asking for profile matches.
	// So its safe to aquire this lock without worry about us stalling the Weighter.
	ca.imgMut.Lock()
	defer ca.imgMut.Unlock()

	// Change seen
	ca.seen += 1

	// Is this the first run?
	//
	// Easy way to tell - there are no images.
	if len(ca.images) == 0 {
		first = true
	}

	// The query should already be prepared at connection.
	fullRows, err := db.Query(bg, "full")
	if err != nil {
		fl.Err(err).Msg("full")
		return err
	}

	for fullRows.Next() {
		// SELECT id, hash, tags FROM files.merged WHERE enabled AND NOT blocked
		if err := fullRows.Scan(&id, &hash, &tgs); err != nil {
			fullRows.Close()
			fl.Err(err).Msg("full-rows-scan")
			return err
		}

		// Don't assume the database doesn't have duplicates and is sorted properly.
		tgs = tgs.Fix()

		// Does this hash already exist?
		img, ok := ca.images[id]
		if !ok {
			// Nope, first one - Go ahead and create it.
			img = &cacheImage{
				ID:   id,
				Hash: hash,
				Tags: tgs.Fix(),
				changed: true,
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
			img.changed = true
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

	if we.yc, err = yconf.New(we.cPath, ycc, &we.l); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	// Run a simple once-through check, not the full Start() yet.
	if err = we.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", we.yc.Get()).Send()

	// Get the loaded configuration
	co, ok := we.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

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
	// mean only updated files would apply these new rules.
	if ucBits&(ucDBConn|ucDBQuery|ucTagRules|ucProfiles) != 0 {
		// Something changed that should force a full
		go we.doFull()
	}

	// Note - We did not check ucPullInt here, thats handled in the partial loop and it will adjust on its next patial run.
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

	// The profiles.
	for name, cProf := range in.Profiles {
		// The Any, All and None we want to convert into a TagRule with the "Tag" given being the profile name.
		// Note that we will never actually assign this tag, just used for matching.
		ctr := tags.ConfTagRule{
			Tag: name,
			Any: cProf.Any,
			All: cProf.All,
			None: cProf.None,
		}

		tr, err := tags.ConfMakeTagRule(&ctr, we.tm)
		if err != nil {
			return nil, err
		}

		cp := &confProfile{
			Matches: tr,
			Name: name,
		}

		if len(cp.Weights) > 0 {
			cp.Weights, err = tags.ConfMakeTagWeights(cProf.Weights, we.tm)
			if err != nil {
				return nil, err
			}
		}
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

	// If this isn't a reload, then nothing further to do.
	if !reload {
		return true, 0
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

	if db, err = pgxpool.ConnectConfig(bg, poolConf); err != nil {
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
	if _, err := db.Prepare(bg, "full", qu.Full); err != nil {
		fl.Err(err).Msg("full")
		return err
	}

	if _, err := db.Prepare(bg, "poll", qu.Poll); err != nil {
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

// func Weighter.loopy {{{

// Handles our basic background tasks, partial and full queries.
func (we *Weighter) loopy() {
	var errors uint32 = 0

	fl := we.l.With().Str("func", "loopy").Logger()

	// We need to know how often we poll.
	co := we.getConf()

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
		case _, ok := <-we.bye:
			if !ok {
				fl.Debug().Msg("Shutting down")
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
				nextPoll.Reset(pollInt * time.Duration(time.Second * time.Duration(errors)))
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

// func Weighter.Close {{{

// Stops all background processing and disconnects from the database.
func (we *Weighter) Close() {
	fl := we.l.With().Str("func", "Close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&we.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	// Shutdown background goroutines
	close(we.bye)

	// Don't need to watch configuration anymore.
	we.yc.Stop()

	if db, err := we.getDB(); err == nil {
		db.Close()
	}

	fl.Info().Msg("closed")
} // }}}
