package weighter

import (
	"context"
	"frame/tags"
	"frame/types"
	"frame/yconf"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// type Weighter struct {{{

type Weighter struct {
	l zerolog.Logger

	// Our image cache
	//
	// No lock is needed to use cache, though it has multiple locks within.
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

	// A whitelist of all the tags we care about.
	// 
	// Any image loaded from the database that does not have at least one
	// of these tags will be ignored and not loaded into our cache.
	//
	// This is updated when the configuration is successfully loaded via doFull()
	//
	// This is actually a tags.Tags, stored in an atomic.Value.
	//
	// Once created it is read-only, and fully replaced when it changes (not modified).
	white atomic.Value

	// Used to control shutting down background goroutines.
	ctx context.Context
} // }}}

type confQueries struct {
	Full string `yaml:"full"`
	Poll string `yaml:"poll"`
}

// type cacheImage struct {{{

// The images loaded from the merged table in the database.
//
// We have a full which pulls all rows, and a poll query which only pulls the most
// recent changes to keep us in sync with the table.
type cacheImage struct {
	// The database ID.
	ID uint64

	Hash string

	// Our combined tags from all the files with the same hash, as well as our tag rules.
	Tags tags.Tags

	// Lets us know if the image we seen by the full query or not.
	//
	// We do not care if this wraps, as each time fullQuery() is run it changes the number
	// in cache.seen and only cares if its the same as this or not.
	seen uint8
} // }}}

// type weightList struct {{{

// See cacheProfile. Weights for more details on how this structure works.
type weightList struct {
	Weight int
	Start  int
	IDs    []uint64
} // }}}

// type cacheProfile struct {{{

type cacheProfile struct {
	// The profile name
	Profile string

	// So details on how this works -
	//
	// This is a sorted list of all the weights for this specific profile.
	//
	// All images that have the same weight are stored in the IDs.
	//
	// To figure out which image to use, we choose a random number between 0 and MaxRoll.
	// Then we search (typically binary search) for which weightList matches that number.
	// It has to be >= that weightList.Start and lower then the next weightList.Start.
	//
	// Once we've found the right weightList to use, we just choose a random number between
	// 0 and len(weightList.IDs) to pick the actual image to use within that weight.
	Weights []*weightList

	MaxRoll int

	// The TagRule that must apply for this image to be considered for inclusion in this profile or not.
	TagRule tags.TagRule
} // }}}

// type cache struct {{{

type cache struct {
	// Our images from the database, the key is the ID assigned by the database.
	//
	// imgMut is a mutex locking only the images map, any change to any image (additions, removals, and changing tags)
	// will happen to a new cacheImage, so anything pointing to any existing cacheImage can continue to do so knowing once created these
	// are read-only and no locking is needed on reading those.
	//
	// The mutex is a RWMutex as we want to allowing reading locks on the map itself during profile generation.
	// A write lock is only gotten when we run queries to get images from the database.
	//
	// Again, the mutex is only needed when accesing the images map. Profiles can save the cacheImage references and access those directly
	// without the lock.
	imgMut sync.RWMutex
	images map[uint64]*cacheImage

	// Used by the full query to set cacheImage.seen to know what images were seen or not so they can be removed.
	// You need the imgMut lock to access this.
	seen uint8

	// Used by polling to mark all the IDs that were changed.
	//
	// Delete/Disabled IDs are also in this list, looking up a disabled ID from images would be how to know it was removed.
	//
	// You need the imgMut lock to access this.
	pollChanged []uint64

	// pMut works much the same as imgMut above - Only needed to access the profiles map itself, and again cacheProfile is considered read-only once
	// it is created. All changes to it will be done to a new cacheProfile and the map will be updated with that.
	pMut     sync.RWMutex
	profiles map[string]*cacheProfile
} // }}}

// type confProfile struct {{{

type confProfile struct {
	Name    string
	Matches tags.TagRule
	Weights tags.TagWeights
} // }}}

// type confProfileYAML struct {{{

type confProfileYAML struct {
	// Images must have at least 1 of these tags to be included.
	// This does not assign any weight to the tag itself, only there to allow/disallow an image for consideration
	// in the profile itself.
	Any []string `yaml:"any"`

	// Images must have all of these tags to be included.
	//
	// Like any above, this does not assign any weight to the tag itself.
	All []string `yaml:"all"`

	// Image must not have any of these tags to be included in the profile.
	None []string `yaml:"none"`

	// The various tags and weights assigned to each tag for the profile.
	//
	// A profile must have a minimum of 1 weighted tag that is greater then 1.
	//
	// The value of the weights themselves is user-defined. You can assigned small or large numbers, you can also assigned negative numbers.
	// Negative numbers are useful for lowering the weight of an image based on the existance of less desirable tags, but still included.
	//
	// Any image to be included must have a final weight of 1 or higher.
	//
	// It is possible to exclude images simply by making their weight less then 1.
	Weights tags.ConfTagWeights `yaml:"weights"`
} // }}}

// type confYAML struct {{{

type confYAML struct {
	Database string `yaml:"database"`

	Queries confQueries `yaml:"queries"`

	Profiles map[string]confProfileYAML `yaml:"profile"`

	// Additional tag rules we apply to images before running any of the images through profiles.
	//
	// Note that these tagrules are not caches and always run when an image is loaded.
	//
	// For best performance put as many of these rules as possible into cmerge rather then here.
	TagRules tags.ConfTagRules `yaml:"tagrules"`

	// Every interval we run the Poll query
	PollInterval time.Duration

	// Every interval we run the Full query
	FullInterval time.Duration
} // }}}

// Updated configuration bits
const (
	ucDBConn   = 1 << iota // When the database connection changes
	ucDBQuery  = 1 << iota // When at least one of the database queries change
	ucTagRules = 1 << iota // When TagRules change
	ucProfiles = 1 << iota // When any of the profiles change
	ucPollInt  = 1 << iota
	ucFullInt  = 1 << iota
)

// type conf struct {{{

type conf struct {
	Database string

	Queries confQueries

	TagRules tags.TagRules

	// Our profiles, main reason for our existance.
	Profiles map[string]*confProfile

	// Every interval we run the Poll query
	PollInterval time.Duration

	// Every interval we run the Full query
	FullInterval time.Duration
} // }}}

// Convert and Notify are set in New()
var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &confYAML{} },
	Merge:   yconfMerge,
	Changed: yconfChanged,
}
