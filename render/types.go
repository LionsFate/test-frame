package render

import (
	"context"
	"frame/types"
	"frame/yconf"
	"image"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// type confProfileYAML struct {{{

type confProfileYAML struct {
	Width  int `yaml:"width"`
	Height int `yaml:"height"`

	// The maximmum amount of images, or "depth" to show on a single rendered
	// image. These images get smaller and smaller until eventually we
	// run out of pixels.
	//
	// The default if not set is 6.
	MaxDepth int `yaml:"maxdepth"`

	// This is the profile name passed to Weighter.GetProfile()
	//
	// We do not have any tags ourselves, those are all handled there.
	TagProfile string `yaml:"tagprofile"`

	// How often to write the new output file.
	//
	// Default if unset is every 5 minutes, or "5m".
	WriteInterval time.Duration `yaml:"writeinterval"`

	// The full path and name of the file to output when generating a new image.
	// The file will be written to OutputrFile.tmp and then renamed so
	// no one gets a partially written file.
	OutputFile string `yaml:"outputfile"`
} // }}}

// type confProfile struct {{{

type confProfile struct {
	Size          image.Point
	Depth         int
	TagProfile    string
	WriteInterval time.Duration
	OutputFile    string

	// Lets us know if renderProfile() is already running or not,
	// so we don't try to render the same profile multiple times
	// concurrently.
	running uint32
} // }}}

// type confYAML struct {{{

type confYAML struct {
	// The individual image profiles.
	Profiles []confProfileYAML `yaml:"profiles"`
} // }}}

// type conf struct {{{
type conf struct {
	// We copy these all over the place and they are read-only once created.
	//
	// So just use a reference to save a bit of memory and copy time.
	Profiles []*confProfile
} // }}}

// type renderInterval struct {{{

type renderInterval struct {
	// The next time we are to run the specific check.
	NextRun time.Time

	NextDur time.Duration

	// The configured render interval
	WriteInt time.Duration

	// The profile(s) we want to run for this interval.
	Profiles []*confProfile
} // }}}

// type Render struct {{{

type Render struct {
	l zerolog.Logger

	// We use an atomic for the configuration since we might replace it at any time while another goroutine
	// can be using it.
	co atomic.Value

	we types.Weighter
	cm types.CacheManager

	// Our configuration path.
	//
	// Can also be a single file if you want to store everything in just one file.
	cPath string

	// We are in startup, fixes things like notifyConf() being called too soon.
	start uint32

	// Used to know if the configuration was updated or not.
	//
	// When configuration changes this is changed.
	// Render.loopy() relies on this to know when to regenerate
	// its check intervals.
	//
	// Do not access directly, use atomics.
	updated uint32

	yc *yconf.YConf

	// Used to decide location for new image.
	// Top/Bottom or Left/Right.
	r *rand.Rand

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}
