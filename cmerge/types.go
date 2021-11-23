package cmerge

import (
	"context"
	"frame/tags"
	"frame/types"
	"frame/yconf"
	"github.com/rs/zerolog"
	"sync"
	"sync/atomic"
	"time"
)

type confQueries struct {
	Full    string `yaml:"full"`
	Poll    string `yaml:"poll"`
	Select  string `yaml:"select"`
	Insert  string `yaml:"insert"`
	Update  string `yaml:"update"`
	Disable string `yaml:"disable"`
}

type confYAML struct {
	Database string `yaml:"database"`

	Queries confQueries `yaml:"queries"`

	// Our tag rules, which we apply when merging.
	TagRules tags.ConfTagRules `yaml:"tagrules"`

	// If a file contains any of these tags, they are flagged as blocked
	BlockTags []string

	// Every interval we run the Poll query
	PollInterval time.Duration `yaml:"pollinterval"`

	// Every interval we run the Full query
	FullInterval time.Duration `yaml:"fullinterval"`
}

// Updated configuration bits
const (
	ucDBConn    = 1 << iota // When the database connection changes
	ucDBQuery   = 1 << iota // When at least one of the database queries change
	ucTagRules  = 1 << iota // When TagRules changes
	ucBlockTags = 1 << iota // When BlockTags changes
	ucPollInt   = 1 << iota // When PollInterval changes
	ucFullInt   = 1 << iota // When FullInterval changes
)

type conf struct {
	Database string

	Queries confQueries

	// Our tag rules, which we apply when merging.
	TagRules tags.TagRules

	// If a file contains any of these tags, they are flagged as blocked
	BlockTags tags.Tags

	// Every interval we run the Poll query
	PollInterval time.Duration

	// Every interval we run the Full query
	FullInterval time.Duration
}

type fileCache struct {
	ID   uint64
	Tags tags.Tags
}

// type hashCache struct {{{

type hashCache struct {
	// The database ID.
	ID uint64

	Hash string

	// Our combined tags from all the files with the same hash, as well as our tag rules.
	Tags tags.Tags

	// If this specific hash is blocked or not.
	Blocked bool

	Files map[uint64]*fileCache

	// If this hash should be disabled or not.
	//
	// Once disabled in the DB then it will be removed from our cache.
	Disabled bool

	Changed bool
} // }}}

// type cache struct {{{

type cache struct {
	// As we don't have any reads without writes, this is a Mutex and not a RWMutex.
	cMut   sync.Mutex
	hashes map[string]*hashCache

	// When doing a poll, this is a list of just those hashes that changed from pollQuery(), so
	// we don't have to loop through hashes checking for changes.
	//
	// This also requires having a lock on cMut to access, as these point to the same values
	// in the hashes map above.
	pollChanged map[string]*hashCache
} // }}}

// type CMerge struct {{{

type CMerge struct {
	l zerolog.Logger

	// Our cache, main reason we are all here.
	ca *cache

	// Stores the *pgxpool.Pool
	//
	// We use an atomic because we want to be able to replace the connection while we are running.
	db atomic.Value

	// We use an atomic for the configuration since we might replace it at any time while another goroutine
	// can be using it.
	co atomic.Value

	// Our configuration path.
	//
	// Can also be a single file if you want to store everything in just one file.
	cPath string

	// Do not access directly, use atomics.
	closed uint32

	tm types.TagManager

	yc *yconf.YConf

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}

// Convert and Notify are set in New()
var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &confYAML{} },
	Merge:   yconfMerge,
	Changed: yconfChanged,
}
