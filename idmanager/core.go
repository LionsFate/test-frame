package idmanager

import (
	"context"
	"errors"
	"frame/types"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
)

// func New {{{

func New(confFile string, l *zerolog.Logger, ctx context.Context) (*IDManager, error) {
	var err error

	im := &IDManager{
		l:     l.With().Str("mod", "idmanager").Logger(),
		cFile: confFile,
		ctx:   ctx,
	}

	fl := im.l.With().Str("func", "New").Logger()

	// Load our configuration.
	if err = im.loadConf(); err != nil {
		return nil, err
	}

	// Start background configuration handling.
	im.yc.Start()

	// Background goroutine to watch the context and shut us down.
	go func() {
		<-im.ctx.Done()
		im.close()
	}()

	fl.Debug().Send()

	return im, nil
} // }}}

// func IDManager.setupDB {{{

// This creates all prepared statements, and if everything goes OK replaces ip.db with this provided db.
func (im *IDManager) setupDB(co *conf, db *pgx.Conn) error {
	fl := im.l.With().Str("func", "setupDB").Str("db", co.Database).Logger()

	// No using the database after a shutdown.
	if atomic.LoadUint32(&im.closed) == 1 {
		fl.Debug().Msg("called after shutdown")
		return types.ErrShutdown
	}

	queries := co.Queries

	// Lets prepare all our statements
	if _, err := db.Prepare(im.ctx, "get-id", queries.GetID); err != nil {
		fl.Err(err).Msg("get-id")
		return err
	}

	if _, err := db.Prepare(im.ctx, "get-hash", queries.GetHash); err != nil {
		fl.Err(err).Msg("get-hash")
		return err
	}

	fl.Debug().Msg("prepared")

	return nil
} // }}}

// func IDManager.dbConnect {{{

func (im *IDManager) dbConnect(co *conf) (*pgxpool.Pool, error) {
	var err error
	var db *pgxpool.Pool

	poolConf, err := pgxpool.ParseConfig(co.Database)
	if err != nil {
		return nil, err
	}

	// Set the log level properly.
	cc := poolConf.ConnConfig
	cc.LogLevel = pgx.LogLevelInfo
	cc.Logger = zerologadapter.NewLogger(im.l)

	// So that each connection creates our prepared statements.
	poolConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := im.setupDB(co, conn); err != nil {
			return err
		}

		return nil
	}

	if db, err = pgxpool.ConnectConfig(im.ctx, poolConf); err != nil {
		return nil, err
	}

	return db, nil
} // }}}

// func IDManager.getDB {{{

// Returns the current database pool.
//
// Loads it from an atomic value so that it can be replaced while running without causing issues.
func (im *IDManager) getDB() (*pgxpool.Pool, error) {
	fl := im.l.With().Str("func", "getDB").Logger()

	db, ok := im.db.Load().(*pgxpool.Pool)
	if !ok {
		err := errors.New("Not a pool")
		fl.Warn().Err(err).Send()
		return nil, err
	}

	return db, nil
} // }}}

// func IDManager.close {{{

// Stops all background processing and disconnects from the database.
func (im *IDManager) close() {
	fl := im.l.With().Str("func", "close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&im.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	fl.Info().Msg("closed")

	if db, err := im.getDB(); err == nil {
		if db != nil {
			db.Close()
		}
	}
} // }}}

// func IDManager.GetHash {{{

// Convert the uint64 tag to the tag name (string).
func (im *IDManager) GetHash(in uint64) (string, error) {
	var hash string

	fl := im.l.With().Str("func", "GetHash").Logger()

	if atomic.LoadUint32(&im.closed) == 1 {
		fl.Info().Msg("called after shutdown")
		return "", types.ErrShutdown
	}

	if in == 0 {
		fl.Debug().Msg("empty")
		return "", errors.New("Empty id")
	}

	fl = fl.With().Uint64("key", in).Logger()

	if tmpH, ok := im.hcache.Load(in); ok {
		if hash, ok := tmpH.(string); ok {
			fl.Debug().Str("cache", "hit").Str("hash", hash).Send()
			return hash, nil
		}
	}

	db, err := im.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return "", err
	}

	if err := db.QueryRow(im.ctx, "GetHash", in).Scan(&hash); err != nil {
		fl.Err(err).Msg("db-GetHash")
		return "", err
	}

	fl.Debug().Str("cache", "miss").Str("hash", hash).Send()
	im.hcache.Store(in, hash)

	return hash, nil
} // }}}

// func IDManager.GetID {{{

// Get the ID of a string hash.
func (im *IDManager) GetID(in string) (uint64, error) {
	var id uint64

	fl := im.l.With().Str("func", "GetID").Logger()

	if atomic.LoadUint32(&im.closed) == 1 {
		fl.Info().Msg("called after shutdown")
		return 0, types.ErrShutdown
	}

	in = strings.ToLower(in)
	in = strings.TrimSpace(in)
	if in == "" {
		fl.Debug().Msg("empty")
		return 0, errors.New("Empty tag")
	}

	fl = fl.With().Str("key", in).Logger()

	if tid, ok := im.cache.Load(in); ok {
		if nid, ok := tid.(uint64); ok {
			fl.Debug().Str("cache", "hit").Uint64("id", nid).Send()
			return nid, nil
		}
	}

	db, err := im.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return 0, err
	}

	if err := db.QueryRow(im.ctx, "GetID", in).Scan(&id); err != nil {
		fl.Err(err).Msg("db-GetID")
		return 0, err
	}

	fl.Debug().Str("cache", "miss").Uint64("id", id).Send()
	im.cache.Store(in, id)

	return id, nil
} // }}}
