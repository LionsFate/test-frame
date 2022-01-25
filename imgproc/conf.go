package imgproc

import (
	"errors"
	"fmt"
	"frame/yconf"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
)

// This file contains all functions related to the loading of our configuration files.

// func yconfConvert {{{

func (ip *ImageProc) yconfConvert(inInt interface{}) (interface{}, error) {
	var err error

	fl := ip.l.With().Str("func", "yconfConvert").Logger()

	in, ok := inInt.(*confYAML)
	if !ok {
		return nil, errors.New("not *confYAML")
	}

	out := &conf{
		// No conversion needed here.
		Database: in.Database,
	}

	if in.Queries != nil {
		// We use the same structure between both, so just copy.
		out.Queries = in.Queries
	}

	// Convert MaxResolution, if set.
	if in.MaxResolution != "" {
		num, err := fmt.Sscanf(in.MaxResolution, "%dx%d", &out.MaxResolution.X, &out.MaxResolution.Y)
		if err != nil || num != 2 {
			err = errors.New("invalid MaxResolution")
			fl.Err(err).Str("maxresolution", in.MaxResolution).Send()
			return nil, err
		}
	}

	// Copy over the ImageCache path if its set.
	if in.ImageCache != "" {
		out.ImageCache = in.ImageCache
	}

	// Any file system base paths defined?
	if in.Bases != nil && len(in.Bases) > 0 {
		out.Bases = make(map[int]*confBase, len(in.Bases))
		for path, baseYAML := range in.Bases {
			outBP := &confBase{
				Base: baseYAML.Base,
				Path: path,

				// Default the TagFile here.
				TagFile: "tags.txt",
			}

			// Replace the default TagFile if set.
			if baseYAML.TagFile != "" {
				outBP.TagFile = baseYAML.TagFile
			}

			// If no check interval, default to 5 minutes
			if baseYAML.CheckInt == "" {
				baseYAML.CheckInt = "5m"
			}

			outBP.CheckInt, err = time.ParseDuration(baseYAML.CheckInt)
			if err != nil {
				err = errors.New("invalid checkinterval")
				fl.Err(err).Str("checkinterval", baseYAML.CheckInt).Send()
				return nil, err
			}

			// Set the map in the output base.
			out.Bases[baseYAML.Base] = outBP
		}
	}

	ip.l.Debug().Str("func", "yconfConvert").Interface("out", out).Send()
	return out, nil
} // }}}

// func yconfMerge {{{

func yconfMerge(inAInt, inBInt interface{}) (interface{}, error) {
	var skipBase bool = false

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

	// Merge the queries if needed.
	if inA.Queries != inB.Queries && inB.Queries != nil {
		inA.Queries = inB.Queries
	} else if inA.Queries != nil && inB.Queries != nil {
		// Both have queries, so update any ones that have changed
		if inA.Queries.FilesSelect != inB.Queries.FilesSelect && inB.Queries.FilesSelect != "" {
			inA.Queries.FilesSelect = inB.Queries.FilesSelect
		}

		if inA.Queries.FilesInsert != inB.Queries.FilesInsert && inB.Queries.FilesInsert != "" {
			inA.Queries.FilesInsert = inB.Queries.FilesInsert
		}

		if inA.Queries.FilesUpdate != inB.Queries.FilesUpdate && inB.Queries.FilesUpdate != "" {
			inA.Queries.FilesUpdate = inB.Queries.FilesUpdate
		}

		if inA.Queries.FilesDisable != inB.Queries.FilesDisable && inB.Queries.FilesDisable != "" {
			inA.Queries.FilesDisable = inB.Queries.FilesDisable
		}

		if inA.Queries.PathsSelect != inB.Queries.PathsSelect && inB.Queries.PathsSelect != "" {
			inA.Queries.PathsSelect = inB.Queries.PathsSelect
		}

		if inA.Queries.PathsInsert != inB.Queries.PathsInsert && inB.Queries.PathsInsert != "" {
			inA.Queries.PathsInsert = inB.Queries.PathsInsert
		}

		if inA.Queries.PathsUpdate != inB.Queries.PathsUpdate && inB.Queries.PathsUpdate != "" {
			inA.Queries.PathsUpdate = inB.Queries.PathsUpdate
		}

		if inA.Queries.PathsDisable != inB.Queries.PathsDisable && inB.Queries.PathsDisable != "" {
			inA.Queries.PathsDisable = inB.Queries.PathsDisable
		}
	}

	// Replace the ImageCache
	if inA.ImageCache != inB.ImageCache && inB.ImageCache != "" {
		inA.ImageCache = inB.ImageCache
	}

	// First ensure A has the database if not empty.
	if inA.Database != inB.Database && inB.Database != "" {
		// Since inB is always the latest file opened, overwrite whatever is in inA.
		inA.Database = inB.Database
	}

	// Copy MaxResolution if needed.
	if inA.MaxResolution != inB.MaxResolution {
		if inB.MaxResolution.X > 0 {
			inA.MaxResolution.X = inB.MaxResolution.X
		}

		if inB.MaxResolution.Y > 0 {
			inA.MaxResolution.Y = inB.MaxResolution.Y
		}
	}

	// If inA has no Bases, but inB does - Just copy the map directly.
	if inA.Bases == nil && inB.Bases != nil {
		inA.Bases = inB.Bases
		skipBase = true
	}

	// Does inB have any base paths?
	if !skipBase && inB.Bases != nil && len(inB.Bases) > 0 {
		// Run through inB, merging anything in it.
		for id, base := range inB.Bases {
			if baseA, ok := inA.Bases[id]; ok {
				// Fix the ID, which should not really ever be wrong, but ya know ... Sanity be good.
				if baseA.Base != id {
					baseA.Base = id
				}

				// It is possible for paths within the base to be added before the base itself.
				//
				// This results in an empty main path for the base itself.
				//
				// We account for that here.
				if baseA.Path == "" {
					baseA.Path = base.Path
				}

				// TagFile changed?
				if base.TagFile != baseA.TagFile {
					baseA.TagFile = base.TagFile
				}

				// The CheckInterval can be 0, same type of logic as above.
				// Paths added before the main base create an otherwise empty base.
				if baseA.CheckInt == 0 {
					baseA.CheckInt = base.CheckInt
				}

				continue
			}

			// Doesn't exist in inA, so just copy it as-is
			inA.Bases[id] = base
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

	if origConf.MaxResolution != newConf.MaxResolution {
		return true
	}

	// Queries change?
	if origConf.Queries.FilesSelect != newConf.Queries.FilesSelect {
		return true
	}

	if origConf.Queries.FilesInsert != newConf.Queries.FilesInsert {
		return true
	}

	if origConf.Queries.FilesUpdate != newConf.Queries.FilesUpdate {
		return true
	}

	if origConf.Queries.FilesDisable != newConf.Queries.FilesDisable {
		return true
	}

	if origConf.Queries.PathsSelect != newConf.Queries.PathsSelect {
		return true
	}

	if origConf.Queries.PathsInsert != newConf.Queries.PathsInsert {
		return true
	}

	if origConf.Queries.PathsUpdate != newConf.Queries.PathsUpdate {
		return true
	}

	if origConf.Queries.PathsDisable != newConf.Queries.PathsDisable {
		return true
	}

	if origConf.ImageCache != newConf.ImageCache {
		return true
	}

	if len(origConf.Bases) != len(newConf.Bases) {
		return true
	}

	// Run through the bases
	for _, origBase := range origConf.Bases {
		newBase, ok := newConf.Bases[origBase.Base]
		if !ok {
			return true
		}

		if origBase.TagFile != newBase.TagFile {
			return true
		}
	}

	return false
} // }}}

// func ImageProc.getConf {{{

func (ip *ImageProc) getConf() *conf {
	fl := ip.l.With().Str("func", "getconf").Logger()

	if co, ok := ip.co.Load().(*conf); ok {
		return co
	}

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing conf?")
	return &conf{}
} // }}}

// func ImageProc.checkConf {{{

// Bool is true if everything checked out OK, false otherwise.
// The uint64 are the bits that changed in the configuration.
func (ip *ImageProc) checkConf(co *conf, reload bool) (bool, uint64) {
	var ucBits uint64

	fl := ip.l.With().Str("func", "checkConf").Bool("reload", reload).Logger()

	if co == nil {
		fl.Warn().Msg("no configuration loaded")
		return false, ucBits
	}

	// We need at least one base, else we have nothing to actually do.
	if len(co.Bases) < 1 {
		fl.Warn().Msg("no bases loaded")
		return false, ucBits
	}

	// Basic sanity checks on each base.
	for id, bc := range co.Bases {
		if id == 0 {
			fl.Warn().Msg("Base ID 0 is not valid")
			return false, ucBits
		}

		if bc.Path == "" {
			fl.Warn().Int("base", id).Msg("Base has no path")
			return false, ucBits
		}

		if bc.TagFile == "" {
			fl.Warn().Int("base", id).Msg("Base has no tagfile")
			return false, ucBits
		}

		if bc.CheckInt < time.Second*10 {
			fl.Warn().Int("base", id).Msg("Base checkinterval needs to be 10 seconds or more")
			return false, ucBits
		}
	}

	// We have our queries?
	if co.Queries == nil {
		fl.Warn().Msg("Missing queries")
		return false, ucBits
	}

	if co.Queries.PathsSelect == "" {
		fl.Warn().Msg("Missing queries.paths-select")
		return false, ucBits
	}

	if co.Queries.PathsInsert == "" {
		fl.Warn().Msg("Missing queries.paths-insert")
		return false, ucBits
	}

	if co.Queries.PathsUpdate == "" {
		fl.Warn().Msg("Missing queries.paths-update")
		return false, ucBits
	}

	if co.Queries.PathsDisable == "" {
		fl.Warn().Msg("Missing queries.paths-disable")
		return false, ucBits
	}

	if co.Queries.FilesSelect == "" {
		fl.Warn().Msg("Missing queries.files-select")
		return false, ucBits
	}

	if co.Queries.FilesInsert == "" {
		fl.Warn().Msg("Missing queries.files-insert")
		return false, ucBits
	}

	if co.Queries.FilesUpdate == "" {
		fl.Warn().Msg("Missing queries.files-update")
		return false, ucBits
	}

	if co.Queries.FilesDisable == "" {
		fl.Warn().Msg("Missing queries.files-disable")
		return false, ucBits
	}

	// Sane MaxResolution, no smaller then 720p, there is no upper bound.
	// If its lower then 720, then we default it to 4k.
	if co.MaxResolution.X < 720 {
		co.MaxResolution.X = 3840
	}

	if co.MaxResolution.Y < 720 {
		co.MaxResolution.Y = 3840
	}

	// Everything below here checks for changes between existing and new configuration.
	//
	// If there is no existing then we have nothing to compare against, so work is done.
	oldco, ok := ip.co.Load().(*conf)

	if !ok {
		return true, ucBits
	}

	// Now we check to see what parts of the configuration changed.

	if oldco.MaxResolution != co.MaxResolution {
		ucBits |= ucMaxRes
	}

	if oldco.Database != co.Database {
		ucBits |= ucDBConn
	}

	if oldco.Queries != co.Queries {
		ucBits |= ucDBQuery
	}

	// If the connection changed, we want to do a quick test of it here to ensure we can connect
	// before we accept it as valid.
	if ucBits&ucDBConn != 0 {
		// Ensure we have a database, and perform a basic connection test.
		if co.Database == "" {
			fl.Warn().Msg("Missing database")
			return false, ucBits
		}

		dbConf, err := pgx.ParseConfig(co.Database)
		if err != nil {
			fl.Err(err).Msg("db conf test")
			return false, ucBits
		}

		// Set the log level properly.
		dbConf.LogLevel = pgx.LogLevelInfo
		dbConf.Logger = zerologadapter.NewLogger(ip.l)

		db, err := pgx.ConnectConfig(ip.ctx, dbConf)
		if err != nil {
			fl.Err(err).Msg("db conn test")
			return false, ucBits
		}

		// Ok, disconnect now that we know that works.
		if err = db.Ping(ip.ctx); err != nil {
			fl.Err(err).Msg("db ping test")
			db.Close(ip.ctx)
			return false, ucBits
		}

		// Disconnect our test
		db.Close(ip.ctx)
	}

	return true, ucBits
} // }}}

// func ImageProc.loadConf {{{

// This is called by New() to load the configuration the first time.
func (ip *ImageProc) loadConf() error {
	var err error

	fl := ip.l.With().Str("func", "loadConf").Logger()

	// Copy the default ycCallers, we need to copy this so we can add our own notifications.
	ycc := ycCallers

	ycc.Notify = func() {
		ip.notifyConf()
	}

	ycc.Convert = func(in interface{}) (interface{}, error) {
		return ip.yconfConvert(in)
	}

	if ip.yc, err = yconf.New(ip.cPath, ycc, &ip.l, ip.ctx); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	// Run a simple once-through check, not the full Start() yet.
	if err = ip.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", ip.yc.Get()).Send()

	// Get the loaded configuration
	co, ok := ip.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	// We don't care about the changed bits here, because we know this is the first load.
	good, _ := ip.checkConf(co, false)
	if !good {
		err := errors.New("bad config")
		fl.Err(err).Send()
		return err
	}

	// We need a new database connection before we can add the cache.
	db, err := ip.dbConnect(co)
	if err != nil {
		fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
		return err
	}

	// Get the cache so we can add the bases to it.
	ca := ip.ca

	// As we are going to potentially be adding to the bases map, we need the lock.
	ca.cMut.Lock()
	defer ca.cMut.Unlock()

	for _, base := range co.Bases {
		// Ensure we have a base cache
		if err := ip.addBaseCache(base, ca, db); err != nil {
			fl.Err(err).Msg("base-check")
			return err
		}

		// This should not be able to fail.
		bc, ok := ca.bases[base.Base]
		if !ok {
			err := errors.New("missing cb.bases")
			fl.Err(err).Send()
			return err
		}

		bc.bMut.Lock()

		if bc.tagFile != base.TagFile {
			fl.Info().Int("base", base.Base).Msg("TagFile Updated")
			bc.tagFile = base.TagFile
		}

		if base.Path != bc.path {
			fl.Info().Str("path", base.Path).Msg("Path updated")
			bc.path = base.Path
			bc.bfs = os.DirFS(bc.path)
			bc.force = true
		}

		// Release the lock
		bc.bMut.Unlock()
	}

	// Set the new DB
	ip.db.Store(db)

	// Store the configuration.
	ip.co.Store(co)

	return nil
} // }}}

// func ImageProc.notifyConf {{{

func (ip *ImageProc) notifyConf() {
	fl := ip.l.With().Str("func", "notifyConf").Logger()

	// Update our configuration.
	co, ok := ip.yc.Get().(*conf)
	if !ok {
		fl.Warn().Msg("Get failed")
		return
	}

	good, ucBits := ip.checkConf(co, true)
	if !good {
		fl.Warn().Msg("invalid configuration - running off old configuration")
		return
	}

	if ucBits&(ucDBConn|ucDBQuery) != 0 {
		db, err := ip.dbConnect(co)
		if err != nil {
			fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
			return
		}

		// Get the old DB (if it exists, first time it won't be set).
		oldDB, ok := ip.db.Load().(*pgxpool.Pool)

		// Set the new DB
		ip.db.Store(db)

		// Close the old DB if it was set, now that the new one has replaced it.
		if ok {
			// We do this in the background, as anyone who is using it will block the Close() from returning.
			go oldDB.Close()
		}

		// Since the database bits have been taken care of, clear those out.
		if ucBits&ucDBConn != 0 {
			ucBits ^= ucDBConn
		}

		if ucBits&ucDBQuery != 0 {
			ucBits ^= ucDBQuery
		}

		// As something changed with the database, we need to refresh our cache.
		if err := ip.loadCache(co); err != nil {
			fl.Err(err).Msg("refreshing cache")
		}
	}

	// Store the new configuration
	ip.co.Store(co)

	// Store the update bits
	atomic.StoreUint64(&ip.ucBits, ucBits)

	fl.Info().Msg("configuration updated")
} // }}}
