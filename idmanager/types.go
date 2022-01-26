package idmanager

import (
	"context"
	"frame/yconf"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type conf struct {
	Database string      `yaml:"database"`
	Queries  confQueries `yaml:"queries"`
}

type confQueries struct {
	GetID   string `yaml:"getid"`
	GetHash string `yaml:"gethash"`
}

// type IDManager struct {{{

type IDManager struct {
	l zerolog.Logger

	yc *yconf.YConf

	// Our internal ID cache, so we only hit the database once per key.
	cache sync.Map

	// Reverse, hash cache.
	// Only used when GetHash() is called, not populated by GetID() since
	// a reverse lookup is not typical from the same program.
	hcache sync.Map

	// Stores the *pgxpool.Pool
	//
	// We use an atomic because we want to be able to replace the connection while we are running.
	db atomic.Value

	cFile string

	// Do not access directly, use atomics.
	closed uint32

	// Lets us know to shutdown.
	ctx context.Context

	co atomic.Value
} // }}}
