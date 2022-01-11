package imgproc

import (
	"context"
	"frame/tags"
	"frame/types"
	"frame/yconf"
	"image"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type confBaseYAML struct {
	Base int      `yaml:"base"`
	Tags []string `yaml:"tags"`

	// The time between when we check the base for changes.
	// Minimum is 30 seconds for sanity, no maximum.
	//
	// Default if not set is 5 minutes.
	//
	// This is anything valid that time.ParseDuration() accepts.
	CheckInt string `yaml:"checkinterval"`
}

type confPathYAML struct {
	Path string   `yaml:"path"`
	Base int      `yaml:"base"`
	Tags []string `yaml:"tags"`
}

type confQueries struct {
	FilesSelect  string `yaml:"files-select"`
	FilesInsert  string `yaml:"files-insert"`
	FilesUpdate  string `yaml:"files-update"`
	FilesDisable string `yaml:"files-disable"`

	PathsSelect  string `yaml:"paths-select"`
	PathsInsert  string `yaml:"paths-insert"`
	PathsUpdate  string `yaml:"paths-update"`
	PathsDisable string `yaml:"paths-disable"`
}

// Pre-converted YAML-friendly configuration.
type confYAML struct {
	MaxResolution string                   `yaml:"maxresolution"`
	Hash          string                   `yaml:"hash"`
	Database      string                   `yaml:"database"`
	ImageCache    string                   `yaml:"imagecache"`
	Queries       *confQueries             `yaml:"queries"`
	Bases         map[string]*confBaseYAML `yaml:"bases"`
	Paths         []*confPathYAML          `yaml:"paths"`
}

type confPath struct {
	Tags tags.Tags
}

type confBase struct {
	Base     int
	Path     string
	Tags     tags.Tags
	CheckInt time.Duration
	Paths    map[string]*confPath
}

type conf struct {
	MaxResolution image.Point
	Hash          int
	ImageCache    string
	Bases         map[int]*confBase
	Queries       *confQueries
	Database      string
}

// What is generally needed for the functions within the check() line.
type checkRun struct {
	hash          int
	maxResolution image.Point
	cachePath     string
	cb            *confBase
	bc            *baseCache
}

// Convert and Notify are set in New(), as they need access to the loaded *ImageProc.
var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &confYAML{} },
	Merge:   yconfMerge,
	Changed: yconfChanged,
}

// type ImageProc struct {{{

type ImageProc struct {
	l zerolog.Logger

	// Stores the *pgxpool.Pool
	//
	// We use an atomic because we want to be able to replace the connection while we are running.
	db atomic.Value

	// The last time gbGet() was called, a time.Time value is stored here.
	//
	// Check that function for details on why this exists.
	dbTime atomic.Value

	cPath string

	co atomic.Value

	ca *cache

	yc *yconf.YConf

	tm types.TagManager

	// The last configuration reload, the bits that changed.
	//
	// Use atomic functions to access and change this value as they are used in multiple locations.
	//
	// Avoding race conditions good.
	ucBits uint64

	// Do not access directly, use atomics.
	closed uint32

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}

// const conf update bits {{{

// Update bits used when the configuration reloads
const (
	ucDBConn  = 1 << iota // When the database connection has changed
	ucDBQuery = 1 << iota // When at least one of the database queries have changed
	ucMaxRes  = 1 << iota // The maximum resolution was changed
	ucBaseCI  = 1 << iota // One of the base check intervals changed
) // }}}

// type checkInterval struct {{{

type checkInterval struct {
	// The next time we are to run the specific check.
	nextRun time.Time

	nextDur time.Duration

	// The configured check interval
	checkInt time.Duration

	// The base(s) we want to run for this check interval.
	//
	// Since by default they all use the same time we expect in most cases we will be running multiple.
	bases []int
} // }}}

// const cache update bits {{{

// Update bits use in fileCache
const (
	// Bits specific to the image files
	upFileTS = 1 << iota // The file modified time
	upFileCT = 1 << iota // The file calculated tags changed
	upFileHS = 1 << iota // The file hash changed

	// Bits specific to image sidecar files
	upSideTS = 1 << iota // The sidecar modified time
	upSideTG = 1 << iota // The sidecar tags

	// Bits specific to pathCache.updated
	upPathTG = 1 << iota // Tags for the path itself changed
	upPathTS = 1 << iota // The directory modified time
	upPathFI = 1 << iota // Files within a path changed
) // }}}

// type fileCache struct {{{

type fileCache struct {
	// Name of the file
	Name string

	// Last updated time for the file itself
	FileTS time.Time

	// Last updated time of the sidecar
	SideTS time.Time

	// Any tags loaded from the sidecar, the .txt or .xmp file.
	SideTG tags.Tags

	// These are the calculated tags - They combine the path tags, and the above file and sidecar tags.
	CTags tags.Tags

	// The files calculated hash
	Hash string

	// If this is set, then the file has some type of error and no further attempt to open it should be attempted.
	//
	// The file however will remain in memory and should the timestamp change, it will be looked at again.
	//
	// When a file is in error condition it also is ignored by any changes to the database.
	// So any existing database status is left as-is.
	//
	// This is helpful for remote content that you have no control over, and are unable to "clean up" any invalid files, and
	// don't want them to continue to produce errors.
	fileError bool

	// A bitflag that says what specifically was update this loop.
	//
	// Helps in knowing exactly what columns in the database changed, if we need to rehash, etc.
	updated uint32

	// What loop we last saw this file on
	loopF uint32

	// What loop we last saw the sidecar on
	loopS uint32

	// If the file is disabled in the database or not.
	disabled bool

	// The ID in the database for this specific file entry, used in UPDATE queries.
	id uint64
} // }}}

// type pathCache struct {{{

// Per-path cache.
type pathCache struct {
	Path    string
	Changed time.Time

	Tags tags.Tags

	Files map[string]*fileCache

	// If the path is disabled in the database or not.
	disabled bool

	// The ID in the database.
	id uint64

	// If a change is detected during check(), updated is set to what changed.
	updated uint32

	// What loop we last saw this path on
	loop uint32
} // }}}

// type baseCache struct {{{

// Per-base cache.
//
// At some point I expect each base will have its own cache settings, but we are not there yet.
//
// This would make things easier for setting separate bases to different intervals, timeouts, etc.
type baseCache struct {
	// When updating the cache, only 1 goroutine at a time can touch the baseCache in any way.
	//
	// Normally every X-interval we launch another goroutine to update the cache, however the prior one may
	// not have finished due to any number of reasons.
	//
	// Because of this, we do not use a Mutex (which will block a goroutine), but rather we use an atomic
	// to get a "lock" via checkRun and setting the value to 1.
	//
	// If a goroutine is still running, it has the atomic and any future goroutines will simply return.
	// This prevents any possibility of goroutines backing up.
	//
	// This has happened to me when running on an older Raspberry PI, where the interval was
	// check every 30 seconds, but due to the number of files on the SD card it often took
	// longer then 30 seconds when changes were found to finish.
	//
	// This resulted in the obvious backup of goroutines, hence no goroutine blocking Mutex here.
	//
	// Though once checkRun has been aquired, it still gets a Mutex lock because configuration changes
	// still need to update the structure (Tags, path, etc).
	checkRun uint32

	bMut sync.Mutex

	// Setting force will ensure that the next check run will check all files for changes.
	// This typically happens if something in the configuration changes, like the path or tags.
	force bool

	// Base ID
	Base int

	Checked time.Time

	Tags tags.Tags

	// The original path to bfs from the configuration, used only to check for changes.
	path string

	// How to access the base itself.
	bfs fs.FS

	// Which loop we are on.
	//
	// This changes every time check() is run, and lets us know which structures we have
	// seen and which we haven't.
	//
	// Very useful for knowing what was removed.
	//
	// We do not care what this value is nor if this value wraps.
	// The only thing that we care about is that its not the same as the last time.
	loop uint32

	// Paths within bfs
	Paths map[string]*pathCache
} // }}}

// type cache struct {{{

type cache struct {
	cMut  sync.Mutex
	bases map[int]*baseCache
} // }}}
