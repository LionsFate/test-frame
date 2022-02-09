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
}

type conf struct {
	MaxResolution image.Point
	ImageCache string
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

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}
