package cmanager

import (
	"context"
	"frame/types"
	"frame/yconf"
	"image"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

type confYAML struct {
	MaxResolution string `yaml:"maxresolution"`
	ImageCache    string `yaml:"imagecache"`

	// This is a boolean setting that when enabled will throttle
	// CacheManager to "be nice" to both the CPU and RAM.
	//
	// Specifically it will use a global mutex on all image Cache/Load
	// functions.
	//
	// In my case while developing this I used a fairly old server that
	// was doing other things beside just this.
	//
	// I also had multiple bases in image processor and multiple
	// images being rendered at the same time.
	//
	// Initial loading of 500k of my images brought the server to a crawl.
	// This throttles that, and once set can be disabled.
	//
	// This will not cause any issues if toggled on/off while running,
	// other then with it off (default) expect more resources to be used.
	BeNice bool `yaml:"benice"`
}

type conf struct {
	MaxResolution image.Point
	ImageCache    string
	BeNice bool
}

// type CManager struct {{{

type CManager struct {
	l zerolog.Logger

	yc *yconf.YConf

	// Our configuration file.
	cFile string

	// We do not have any cache of ID or hashes, as if any repeat
	// calls are made for the same ID/hash, IDManager has its own
	// cache that we rely upon. No need to re-invent.

	// Our configuration.
	co atomic.Value

	im types.IDManager

	// Pool for our bytes.Buffer
	bp sync.Pool

	// Used to assign a uinque ID for each log line in the same function.
	// Helps some confusion when figuring things out.
	//
	// Only accessed using atomics.
	c uint64

	// If the BeNice configuration option is set, this mutex
	// is called around all Cache/Load functions.
	beNice sync.Mutex

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}
