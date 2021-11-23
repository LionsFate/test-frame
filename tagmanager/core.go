package tagmanager

import (
	"context"
	"errors"
	"frame/types"
	"frame/yconf"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
	"strings"
	"sync"
	"sync/atomic"
)

type conf struct {
	Database string `yaml:"database"`
}

// type TagManager struct {{{

type TagManager struct {
	l zerolog.Logger

	// Our internal tag cache, so we only hit the database once per key.
	cache sync.Map

	// Reverse, name cache.
	// Only used when Name() is called, not otherwise populated by other functions such as Get().
	ncache sync.Map

	// Stores the *pgxpool.Pool
	//
	// We use an atomic because we want to be able to replace the connection while we are running.
	db atomic.Value

	cFile string

	// Do not access directly, use atomics.
	closed uint32

	// Lets us know to shutdown.
	ctx context.Context

	co *conf
} // }}}

var ycCallers = yconf.Callers{
	Empty: func() interface{} { return &conf{} },
}

// func New {{{

func New(confFile string, l *zerolog.Logger, ctx context.Context) (*TagManager, error) {
	var err error

	tm := &TagManager{
		l:     l.With().Str("mod", "tagmanager").Logger(),
		cFile: confFile,
		ctx:   ctx,
	}

	fl := tm.l.With().Str("func", "New").Logger()

	// Load our configuration.
	if err = tm.loadConf(); err != nil {
		return nil, err
	}

	if err = tm.dbConnect(tm.co.Database); err != nil {
		fl.Err(err).Msg("Connect")
		return nil, err
	}

	// Background goroutine to watch the context and shut us down.
	go func() {
		<-tm.ctx.Done()
		tm.close()
	}()

	return tm, nil
} // }}}

// func TagManager.dbConnect {{{

func (tm *TagManager) dbConnect(uri string) error {
	var err error
	var db *pgxpool.Pool

	poolConf, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return err
	}

	// Set the log level properly.
	cc := poolConf.ConnConfig
	cc.LogLevel = pgx.LogLevelInfo
	cc.Logger = zerologadapter.NewLogger(tm.l)

	// So that each connection creates our prepared statements.
	poolConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Prepare(ctx, "GetID", "SELECT tags.get_tagid($1)"); err != nil {
			return err
		}

		if _, err := conn.Prepare(ctx, "GetName", "SELECT name FROM tags.tags WHERE tid = $1"); err != nil {
			return err
		}

		return nil
	}

	if db, err = pgxpool.ConnectConfig(tm.ctx, poolConf); err != nil {
		return err
	}

	// Get the old DB (if it exists, first time it won't be set).
	oldDB, ok := tm.db.Load().(*pgxpool.Pool)

	// Set the new DB
	tm.db.Store(db)

	// Close the old DB if it was set, now that the new one has replaced it.
	if ok {
		// We do this in the background, as anyone who is using it will block the Close() from returning.
		go oldDB.Close()
	}

	return nil
} // }}}

// func TagManager.getDB {{{

// Returns the current database pool.
//
// Loads it from an atomic value so that it can be replaced while running without causing issues.
func (tm *TagManager) getDB() (*pgxpool.Pool, error) {
	fl := tm.l.With().Str("func", "getDB").Logger()

	db, ok := tm.db.Load().(*pgxpool.Pool)
	if !ok {
		err := errors.New("Not a pool")
		fl.Warn().Err(err).Send()
		return nil, err
	}

	return db, nil
} // }}}

// func TagManager.loadConf {{{

func (tm *TagManager) loadConf() error {
	fl := tm.l.With().Str("func", "loadConf").Logger()

	yc, err := yconf.New(tm.cFile, ycCallers, &tm.l, tm.ctx)
	if err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	if err = yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", yc.Get()).Send()

	// Get the loaded configuration
	if co, ok := yc.Get().(*conf); ok {
		tm.co = co
	}

	if tm.co == nil || tm.co.Database == "" {
		err := errors.New("Missing database")
		fl.Err(err).Send()
		return err
	}

	return nil
} // }}}

// func TagManager.close {{{

// Stops all background processing and disconnects from the database.
func (tm *TagManager) close() {
	fl := tm.l.With().Str("func", "close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&tm.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	fl.Info().Msg("closed")

	if db, err := tm.getDB(); err == nil {
		if db != nil {
			db.Close()
		}
	}
} // }}}

// func TagManager.Name {{{

// Convert the uint64 tag to the tag name (string).
func (tm *TagManager) Name(in uint64) (string, error) {
	var name string

	fl := tm.l.With().Str("func", "Name").Logger()

	if atomic.LoadUint32(&tm.closed) == 1 {
		fl.Info().Msg("called after shutdown")
		return "", types.ErrShutdown
	}

	if in == 0 {
		fl.Debug().Msg("empty")
		return "", errors.New("Empty id")
	}

	fl = fl.With().Uint64("key", in).Logger()

	if tn, ok := tm.ncache.Load(in); ok {
		if name, ok := tn.(string); ok {
			fl.Debug().Str("cache", "hit").Str("name", name).Send()
			return name, nil
		}
	}

	db, err := tm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return "", err
	}

	if err := db.QueryRow(tm.ctx, "GetName", in).Scan(&name); err != nil {
		fl.Err(err).Msg("GetName")
		return "", err
	}

	fl.Debug().Str("cache", "miss").Str("name", name).Send()
	tm.ncache.Store(in, name)

	return name, nil
} // }}}

// func TagManager.Get {{{

// Get the ID of a string tag.
func (tm *TagManager) Get(in string) (uint64, error) {
	var id uint64

	fl := tm.l.With().Str("func", "Get").Logger()

	if atomic.LoadUint32(&tm.closed) == 1 {
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

	if tid, ok := tm.cache.Load(in); ok {
		if nid, ok := tid.(uint64); ok {
			fl.Debug().Str("cache", "hit").Uint64("id", nid).Send()
			return nid, nil
		}
	}

	db, err := tm.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return 0, err
	}

	if err := db.QueryRow(tm.ctx, "GetID", in).Scan(&id); err != nil {
		fl.Err(err).Msg("GetID")
		return 0, err
	}

	fl.Debug().Str("cache", "miss").Uint64("id", id).Send()
	tm.cache.Store(in, id)

	return id, nil
} // }}}
