// Handlers merging the file cache into a usable cache for display.
package cmerge

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

	if inA.Queries.Select != inB.Queries.Select && inB.Queries.Select != "" {
		inA.Queries.Select = inB.Queries.Select
	}

	if inA.Queries.Insert != inB.Queries.Insert && inB.Queries.Insert != "" {
		inA.Queries.Insert = inB.Queries.Insert
	}

	if inA.Queries.Update != inB.Queries.Update && inB.Queries.Update != "" {
		inA.Queries.Update = inB.Queries.Update
	}

	if inA.Queries.Disable != inB.Queries.Disable && inB.Queries.Disable != "" {
		inA.Queries.Disable = inB.Queries.Disable
	}

	if len(inB.BlockTags) > 0 && !inA.BlockTags.Equal(inB.BlockTags) {
		inA.BlockTags = inA.BlockTags.Combine(inB.BlockTags)
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

	if origConf.Queries.Select != newConf.Queries.Select {
		return true
	}

	if origConf.Queries.Insert != newConf.Queries.Insert {
		return true
	}

	if origConf.Queries.Update != newConf.Queries.Update {
		return true
	}

	if origConf.Queries.Disable != newConf.Queries.Disable {
		return true
	}

	if !origConf.BlockTags.Equal(newConf.BlockTags) {
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

func New(confPath string, tm types.TagManager, l *zerolog.Logger) (*CMerge, error) {
	var err error

	cm := &CMerge{
		l:     l.With().Str("mod", "cmerge").Logger(),
		tm:    tm,
		cPath: confPath,
		bye:   make(chan struct{}, 0),

		// Do not create the hashes, we only add ca here for the mutex.
		// The hashes is created in doFull()
		ca: &cache{},
	}

	fl := cm.l.With().Str("func", "New").Logger()

	// Load our configuration.
	//
	// This also handles connecting to the database.
	if err = cm.loadConf(); err != nil {
		return nil, err
	}

	fl.Debug().Send()

	// Do 1 full before we return to ensure everything is running correctly.
	//
	// The first time this can take a while, but tends to be a whole lot faster after.
	cm.doFull()

	// Start background processing to watch configuration for changes.
	cm.yc.Start()

	// Start the loop.
	go cm.loopy()

	fl.Debug().Send()

	return cm, nil
} // }}}

// func CMerge.doPoll {{{

func (cm *CMerge) doPoll() error {
	fl := cm.l.With().Str("func", "doPoll").Logger()
	fl.Debug().Send()

	ca := cm.ca

	// Lock the cache
	ca.cMut.Lock()
	defer ca.cMut.Unlock()

	db, err := cm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	if err := cm.pollQuery(); err != nil {
		return err
	}

	// Start a transaction.
	tx, err := db.Begin(bg)
	if err != nil {
		fl.Err(err).Msg("Begin")
		return err
	}

	if err := cm.pollMerge(tx); err != nil {
		fl.Err(err).Msg("pollMerge")
		tx.Rollback(bg)
		return err
	}

	if err := tx.Commit(bg); err != nil {
		fl.Err(err).Msg("commit")
		return err
	}
	return nil
} // }}}

// func CMerge.doFull {{{

func (cm *CMerge) doFull() error {
	fl := cm.l.With().Str("func", "doFull").Logger()

	ca := cm.ca

	// Lock the cache
	ca.cMut.Lock()
	defer ca.cMut.Unlock()

	// When we do a full we want a clear plate, so we wipe away any existing cache before we begin.
	if len(ca.hashes) > 0 {
		fl.Info().Msg("clearning cache for full")
	}

	ca.hashes = make(map[string]*hashCache, 1)

	// Get the existing merged table (if any) before anything else.
	if err := cm.selectMerged(); err != nil {
		fl.Err(err).Msg("pull")
		return err
	}

	// Pull all the files from the files table.
	if err := cm.fullQuery(); err != nil {
		return err
	}

	db, err := cm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// Start a transaction.
	tx, err := db.Begin(bg)
	if err != nil {
		fl.Err(err).Msg("Begin")
		return err
	}

	// Merge the files into our file hash.
	if err := cm.fullMerge(tx); err != nil {
		fl.Err(err).Msg("fullMerge")
		return err
	}

	if err := tx.Commit(bg); err != nil {
		fl.Err(err).Msg("commit")
		return err
	}

	return nil
} // }}}

// func CMerge.selectMerged {{{

// This gets all the existing rows from the merged table, generally only called at startup.
func (cm *CMerge) selectMerged() error {
	var fid uint64
	var hash string
	var tgs tags.Tags
	var blocked bool

	fl := cm.l.With().Str("func", "pullMerged").Logger()

	db, err := cm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// The full query should already be prepared at connection.
	fullRows, err := db.Query(bg, "select")
	if err != nil {
		fl.Err(err).Msg("select")
		return err
	}

	// Get our cache - locking is handled by our caller.
	ca := cm.ca

	for fullRows.Next() {
		// SELECT mid, hash, tags, blocked, enabled FROM files.merged
		if err := fullRows.Scan(&fid, &hash, &tgs, &blocked); err != nil {
			fullRows.Close()
			fl.Err(err).Msg("select-rows-scan")
			return err
		}

		// Don't assume the database doesn't have duplicates and is sorted properly.
		tgs = tgs.Fix()

		// Note that we don't care if we exist already or not, as we are only supposed to be called at startup.
		//
		// Its also possible for us to be called when you want to replace the entire cache, in that case the cache should be
		// emptied before calling this function.
		//
		// As the hash is unique we also don't need to care about merging here.
		ca.hashes[hash] = &hashCache{
			ID:      fid,
			Hash:    hash,
			Tags:    tgs,
			Blocked: blocked,

			// Create the empty Files hash, as we expect something to be adde when we do the full.
			Files: make(map[uint64]*fileCache, 1),
		}
	}

	fullRows.Close()

	return nil
} // }}}

// func CMerge.pollQuery {{{

func (cm *CMerge) pollQuery() error {
	var fid uint64
	var hash string
	var changed, enabled bool
	var tgs tags.Tags

	fl := cm.l.With().Str("func", "pollQuery").Logger()

	db, err := cm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// The full query should already be prepared at connection.
	pollRows, err := db.Query(bg, "poll")
	if err != nil {
		fl.Err(err).Msg("poll")
		return err
	}

	// Clear pollChanged first.

	// Get our cache - locking is handled by our caller.
	ca := cm.ca

	// Clear any previously set pollChanged.
	//
	// Technically this should already be empty, but we like sanity.
	if ca.pollChanged == nil {
		ca.pollChanged = make(map[string]*hashCache, 1)
	}

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
		if err := pollRows.Scan(&fid, &hash, &tgs, &enabled); err != nil {
			pollRows.Close()
			fl.Err(err).Msg("poll-rows-scan")
			return err
		}

		// Don't assume the database doesn't have duplicates and is sorted properly.
		tgs = tgs.Fix()

		// Does this hash already exist?
		hc, ok := ca.hashes[hash]
		if !ok {
			// Nope - Is it enabled?
			//
			// New file that is already disabled? Go ahead and skip it.
			if !enabled {
				continue
			}

			// Nope, first one - Go ahead and create it.
			hc = &hashCache{
				Hash:    hash,
				Blocked: false,
				Files:   make(map[uint64]*fileCache, 1),
			}

			changed = true
			ca.hashes[hash] = hc
		}

		// Is this file new?
		fc, ok := hc.Files[fid]
		if !ok {
			// Enabled?
			if !enabled {
				// Same logic as above, skip this.
				continue
			}

			// File is new, so make it.
			fc = &fileCache{
				ID: fid,
			}

			hc.Files[fid] = fc
			changed = true
		}

		// Should the file be removed?
		if !enabled {
			// Yep, so delete the file fileCache.
			delete(hc.Files, fid)
			changed = true
		}

		// Tags change?
		if !tgs.Equal(fc.Tags) {
			fc.Tags = tgs
			changed = true
		}

		// If this hash changed in some way, add it to pollChanged.
		//
		// Note that duplicates are OK, we expect them to happen occasionally.
		// Two files with the same hash changing in the same updated.
		//
		// It adds a little more work but not a whole lot.
		if changed {
			changed = false
			ca.pollChanged[hash] = hc
		}
	}

	pollRows.Close()

	return nil
} // }}}

// func CMerge.fullQuery {{{

func (cm *CMerge) fullQuery() error {
	var fid uint64
	var hash string
	var tgs tags.Tags

	fl := cm.l.With().Str("func", "fullQuery").Logger()

	db, err := cm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// The full query should already be prepared at connection.
	fullRows, err := db.Query(bg, "full")
	if err != nil {
		fl.Err(err).Msg("full")
		return err
	}

	// Get our cache - locking is handled by our caller.
	ca := cm.ca

	for fullRows.Next() {
		// SELECT fid, hash, tags FROM files.files WHERE enabled
		if err := fullRows.Scan(&fid, &hash, &tgs); err != nil {
			fullRows.Close()
			fl.Err(err).Msg("full-rows-scan")
			return err
		}

		// Does this hash already exist?
		hc, ok := ca.hashes[hash]
		if !ok {
			// Nope, first one - Go ahead and create it.
			hc = &hashCache{
				Hash:    hash,
				Blocked: false,
				Files:   make(map[uint64]*fileCache, 1),
			}

			ca.hashes[hash] = hc
		}

		// Is this file new?
		fc, ok := hc.Files[fid]
		if !ok {
			// File is new, so make it.
			fc = &fileCache{
				ID: fid,
			}

			hc.Files[fid] = fc
		}

		// Tags change?
		if !tgs.Equal(fc.Tags) {
			fc.Tags = tgs
		}

		// We don't calculate anything else here, we just load the rows and sync it up here.
	}

	fullRows.Close()

	return nil
} // }}}

// func CMerge.hashCheck {{{

func (cm *CMerge) hashCheck(hc *hashCache, co *conf) error {
	var tgs tags.Tags
	var block bool

	fl := cm.l.With().Str("func", "hashCheck").Str("hash", hc.Hash).Logger()

	// Ensure we have at least one file for this hash.
	if len(hc.Files) < 1 {
		// Some extra bit of sanity.
		if hc.ID == 0 {
			// Wait - An entry in the hash without an ID?
			//
			// This should not be possible.
			// Only two ways to be added to the hash - We were loaded from the database, in which case we have an ID
			// and if no files are found with the same hash, we get here - An ID and no files.
			//
			// The 2nd way is we don't yet exist in the database, thus an ID of 0, but we have files which caused us
			// to be added to the cache.
			//
			// But to not have any files or an ID means there is a programming error, so this sanity check means
			// we have a bug that needs fixing.
			err := errors.New("hashCache with no files or id")
			fl.Warn().Err(err).Send()
			return err
		}

		// No file? That means sometime after we stored the in the database the file it was generated from was removed.
		hc.Changed = true
		hc.Disabled = true
		return nil
	}

	// Combine all the individual file tags into the hash tags.
	for _, fc := range hc.Files {
		tgs = tgs.Combine(fc.Tags)
	}

	// Now apply the rules in the order they were loaded.
	//
	// Note that we could use TagRules.Apply(), but we want to log when a rule is applied to know better whats going on.
	// This is basically the same as that function though.
	//
	// If we ever remove the debug logging here? Then just switch to that function and remove the range here.
	for _, tr := range co.TagRules {
		if tr.Give(tgs) {

			// For debugging we want the actual tagrule name rather then the uint64 ID, makes things a bit easier.
			if fl.GetLevel() <= zerolog.DebugLevel {
				name, err := cm.tm.Name(tr.Tag)
				if err != nil {
					fl.Debug().Str("hash", hc.Hash).Uint64("tagruleid", tr.Tag).Send()
				} else {
					fl.Debug().Str("hash", hc.Hash).Str("tagrule", name).Send()
				}
			}

			tgs = tgs.Add(tr.Tag)
		}
	}

	// Did the tags change?
	if !hc.Tags.Equal(tgs) {
		fl.Debug().Str("hash", hc.Hash).Msg("tags")
		hc.Changed = true
		hc.Tags = tgs
	}

	// Is this file blocked?
	block = hc.Tags.Contains(co.BlockTags)
	if block != hc.Blocked {
		fl.Debug().Str("hash", hc.Hash).Bool("block", block).Send()
		hc.Changed = true
		hc.Blocked = block
	}

	return nil
} // }}}

// func CMerge.pushHash {{{

func (cm *CMerge) pushHash(hc *hashCache, tx pgx.Tx) error {
	// Any actual work to do?
	if !hc.Changed {
		return nil
	}

	fl := cm.l.With().Str("func", "pushHash").Str("hash", hc.Hash).Logger()

	// Disabled?
	if hc.Disabled {
		// See the logic in fullMerge for details.
		if hc.ID == 0 {
			err := errors.New("hashCache with no files or id")
			fl.Warn().Err(err).Send()
			return err
		}

		if _, err := tx.Exec(bg, "disable", hc.ID); err != nil {
			fl.Err(err).Uint64("id", hc.ID).Msg("disable")
			return err
		}

		// Now remove the hash from our cache.
		delete(cm.ca.hashes, hc.Hash)
		return nil
	}

	// Updating an existing row?
	if hc.ID != 0 {
		// Yep, just apply the changes to the id.
		// UPDATE files.merged SET tags = $1, blocked = $2 WHERE mid = $3
		if _, err := tx.Exec(bg, "update", hc.Tags, hc.Blocked, hc.ID); err != nil {
			fl.Err(err).Uint64("id", hc.ID).Msg("update")
			return err
		}

		// Changes written, so clear Changed.
		hc.Changed = false
		return nil
	}

	// New row, so insert it.
	// INSERT INTO files.mergeed ( hash, tags, blocked ) VALUES ( $1, $2, $3 ) ON CONFLICT ON CONSTRAINT "merged_hash_key" DO UPDATE SET tags = EXCLUDED.tags, blocked = EXCLUDED.blocked, enabled = true RETURNING mid
	if err := tx.QueryRow(bg, "insert", hc.Hash, hc.Tags, hc.Blocked).Scan(&hc.ID); err != nil {
		fl.Err(err).Uint64("id", hc.ID).Msg("insert")
		return err
	}

	// Changes written, so clear Changed.
	hc.Changed = false
	return nil
} // }}}

// func CMerge.pollMerge {{{

// Generally called after pollQuery(), runs through the cache and updates all the tags.
func (cm *CMerge) pollMerge(tx pgx.Tx) error {
	fl := cm.l.With().Str("func", "pollMerge").Logger()
	fl.Debug().Send()

	co := cm.getConf()
	ca := cm.ca

	for _, hc := range ca.pollChanged {
		if err := cm.hashCheck(hc, co); err != nil {
			return err
		}

		// Did the hash change?
		if hc.Changed {
			// Yep, push it to the database.
			if err := cm.pushHash(hc, tx); err != nil {
				return err
			}
		}
	}

	// Clean the map.
	// Its created again in pollQuery() as needed.
	ca.pollChanged = nil

	return nil
} // }}}

// func CMerge.fullMerge {{{

// Generally called after fullQuery(), runs through the cache and updates all the tags.
func (cm *CMerge) fullMerge(tx pgx.Tx) error {
	fl := cm.l.With().Str("func", "fullMerge").Logger()
	fl.Debug().Send()

	co := cm.getConf()
	ca := cm.ca

	for _, hc := range ca.hashes {
		if err := cm.hashCheck(hc, co); err != nil {
			return err
		}

		// Did the hash change?
		if hc.Changed {
			// Yep, push it to the database.
			if err := cm.pushHash(hc, tx); err != nil {
				return err
			}
		}
	}

	return nil
} // }}}

// func CMerge.checkConf {{{

// If the bool returns true then everything was OK and the configuration is good, false otherwise.
//
// The uint64 is the bits that changed.
func (cm *CMerge) checkConf(co *conf, reload bool) (bool, uint64) {
	var ucBits uint64

	fl := cm.l.With().Str("func", "checkConf").Bool("reload", reload).Logger()

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

	if co.Queries.Select == "" {
		fl.Warn().Msg("Missing queries.Select")
		return false, 0
	}

	if co.Queries.Insert == "" {
		fl.Warn().Msg("Missing queries.Insert")
		return false, 0
	}

	if co.Queries.Update == "" {
		fl.Warn().Msg("Missing queries.Update")
		return false, 0
	}

	if co.Queries.Disable == "" {
		fl.Warn().Msg("Missing queries.Disable")
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
	oldco := cm.getConf()

	if co.Database != oldco.Database {
		ucBits |= ucDBConn
	}

	if co.Queries.Full != oldco.Queries.Full {
		ucBits |= ucDBQuery
	}

	if co.Queries.Poll != oldco.Queries.Poll {
		ucBits |= ucDBQuery
	}

	if co.Queries.Select != oldco.Queries.Select {
		ucBits |= ucDBQuery
	}

	if co.Queries.Insert != oldco.Queries.Insert {
		ucBits |= ucDBQuery
	}

	if co.Queries.Update != oldco.Queries.Update {
		ucBits |= ucDBQuery
	}

	if co.Queries.Disable != oldco.Queries.Disable {
		ucBits |= ucDBQuery
	}

	if !co.BlockTags.Equal(oldco.BlockTags) {
		ucBits |= ucBlockTags
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

// func CMerge.loadConf {{{

// This is called by New() to load the configuration the first time.
func (cm *CMerge) loadConf() error {
	var err error

	fl := cm.l.With().Str("func", "loadConf").Logger()

	// Copy the default ycCallers, we need to copy this so we can add our own notifications.
	ycc := ycCallers

	ycc.Notify = func() {
		cm.notifyConf()
	}

	ycc.Convert = func(in interface{}) (interface{}, error) {
		return cm.yconfConvert(in)
	}

	if cm.yc, err = yconf.New(cm.cPath, ycc, &cm.l); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	// Run a simple once-through check, not the full Start() yet.
	if err = cm.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", cm.yc.Get()).Send()

	// Get the loaded configuration
	co, ok := cm.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	// Check the configuration sanity first.
	if good, _ := cm.checkConf(co, false); !good {
		return errors.New("Invalid configuration")
	}

	// Yep, so go ahead and create a new connection and get it prepared to replace the existing one.
	if err := cm.dbConnect(co); err != nil {
		fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
		return err
	}

	// Looks good, go ahead and store it.
	cm.co.Store(co)

	return nil
} // }}}

// func CMerge.notifyConf {{{

func (cm *CMerge) notifyConf() {
	fl := cm.l.With().Str("func", "notifyConf").Logger()

	// Update our configuration.
	co, ok := cm.yc.Get().(*conf)
	if !ok {
		fl.Warn().Msg("Get failed")
		return
	}

	// Check the new configuration before we do anything.
	good, ucBits := cm.checkConf(co, true)

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
		if err := cm.dbConnect(co); err != nil {
			fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
			return
		}
	}

	// Store the new configuration
	cm.co.Store(co)

	// Did anything change that would cause a full to be needed?
	//
	// Note that we include changing any queries or reconnecting as needing a full.
	//
	// This has the side benefit of allowing us at runtime to connect to a new empty database and just carry
	// on without issue.
	//
	// Obviously changing any of the TagRules or BlockTags would force another full, as skipping a full on these would
	// mean only updated files would apply these new rules.
	if ucBits&(ucDBConn|ucDBQuery|ucTagRules|ucBlockTags) != 0 {
		// Something changed that should force a full
		go cm.doFull()
	}

	// Note - We did not check ucPullInt here, thats handled in the loop and it will adjust on its next run.
	fl.Info().Msg("configuration updated")
} // }}}

// func CMerge.yconfConvert {{{

func (cm *CMerge) yconfConvert(inInt interface{}) (interface{}, error) {
	var err error

	fl := cm.l.With().Str("func", "yconfConvert").Logger()
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

	// Blocked tags
	if len(in.BlockTags) > 0 {
		if out.BlockTags, err = tags.StringsToTags(in.BlockTags, cm.tm); err != nil {
			return nil, err
		}
	}

	// TagRules
	if len(in.TagRules) > 0 {
		if out.TagRules, err = tags.ConfMakeTagRules(in.TagRules, cm.tm); err != nil {
			return nil, err
		}
	}

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

// func CMerge.dbConnect {{{

func (cm *CMerge) dbConnect(co *conf) error {
	var err error
	var db *pgxpool.Pool

	poolConf, err := pgxpool.ParseConfig(co.Database)
	if err != nil {
		return err
	}

	// Set the log level properly.
	cc := poolConf.ConnConfig
	cc.LogLevel = pgx.LogLevelInfo
	cc.Logger = zerologadapter.NewLogger(cm.l)

	queries := &co.Queries

	// So that each connection creates our prepared statements.
	poolConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := cm.setupDB(queries, conn); err != nil {
			return err
		}

		return nil
	}

	if db, err = pgxpool.ConnectConfig(bg, poolConf); err != nil {
		return err
	}

	// Get the old DB (if it exists, first time it won't be set).
	oldDB, ok := cm.db.Load().(*pgxpool.Pool)

	// Set the new DB (especially before we close the possible old connection)
	cm.db.Store(db)

	// Close the old DB if it was set, now that the new one has replaced it.
	if ok {
		// We do this in the background, as anyone who is using it will block the Close() from returning.
		go oldDB.Close()
	}

	return nil
} // }}}

// func CMerge.setupDB {{{

// This creates all prepared statements, and if everything goes OK replaces cm.db with this provided db.
func (cm *CMerge) setupDB(qu *confQueries, db *pgx.Conn) error {
	fl := cm.l.With().Str("func", "setupDB").Logger()

	// No using the database after a shutdown.
	if atomic.LoadUint32(&cm.closed) == 1 {
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

	if _, err := db.Prepare(bg, "select", qu.Select); err != nil {
		fl.Err(err).Msg("select")
		return err
	}

	if _, err := db.Prepare(bg, "insert", qu.Insert); err != nil {
		fl.Err(err).Msg("insert")
		return err
	}

	if _, err := db.Prepare(bg, "update", qu.Update); err != nil {
		fl.Err(err).Msg("update")
		return err
	}

	if _, err := db.Prepare(bg, "disable", qu.Disable); err != nil {
		fl.Err(err).Msg("disable")
		return err
	}

	fl.Debug().Msg("prepared")

	return nil
} // }}}

// func CMerge.getDB {{{

// Returns the current database pool.
//
// Loads it from an atomic value so that it can be replaced while running without causing issues.
func (cm *CMerge) getDB() (*pgxpool.Pool, error) {
	fl := cm.l.With().Str("func", "getDB").Logger()

	db, ok := cm.db.Load().(*pgxpool.Pool)
	if !ok {
		err := errors.New("Not a pool")
		fl.Warn().Err(err).Send()
		return nil, err
	}

	return db, nil
} // }}}

// func CMerge.getConf {{{

func (cm *CMerge) getConf() *conf {
	fl := cm.l.With().Str("func", "getConf").Logger()

	if co, ok := cm.co.Load().(*conf); ok {
		return co
	}

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing conf?")
	return &conf{}
} // }}}

// func CMerge.loopy {{{

// Handles our basic background tasks, full and poll queries.
func (cm *CMerge) loopy() {
	var errors uint32 = 0

	fl := cm.l.With().Str("func", "loopy").Logger()

	// We need to know how often we poll.
	co := cm.getConf()

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
		case _, ok := <-cm.bye:
			if !ok {
				fl.Debug().Msg("Shutting down")
				return
			}
		case <-nextPoll.C:
			// Get the configuration and check if PollInterval changed
			co = cm.getConf()

			if co.PollInterval != pollInt {
				// It changed, so reset the ticker.
				fl.Info().Msg("Updated PollInterval")
				pollInt = co.PollInterval
				nextPoll.Reset(pollInt)
			}

			// Run a pull.
			if err := cm.doPoll(); err != nil {
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
			co = cm.getConf()

			if co.FullInterval != fullInt {
				// It changed, so reset the ticker.
				fl.Info().Msg("Updated FullInterval")
				fullInt = co.FullInterval
				nextFull.Reset(fullInt)
			}

			// Run a full.
			if err := cm.doFull(); err != nil {
				fl.Err(err).Msg("doFull")
			}
		}
	}
} // }}}

// func CMerge.Close {{{

// Stops all background processing and disconnects from the database.
func (cm *CMerge) Close() {
	fl := cm.l.With().Str("func", "Close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&cm.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	// Shutdown background goroutines
	close(cm.bye)

	// Don't need to watch configuration anymore.
	cm.yc.Stop()

	if db, err := cm.getDB(); err == nil {
		db.Close()
	}

	fl.Info().Msg("closed")
} // }}}
