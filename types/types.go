package types

import (
	"errors"
	"frame/tags"
)

var ErrShutdown = errors.New("Shutdown")

// type WeighterProfile interface {{{

type WeighterProfile interface {
	// Returns the requested number of random file IDs from the profile.
	//
	// This is a uint8 specifically because we do not plan on returning too
	// many at any one time.
	//
	// Currently the maximum is 100, about 10x more then what could be
	// considered normal for a single image.
	Get(uint8) ([]uint64, error)
} // }}}

// type Weighter interface {{{

type Weighter interface {
	// This returned (if exists) a specific Weighter profile that
	// can be used to ask for one or more files (hashes) that match that profile.
	GetProfile(string) (WeighterProfile, error)
} // }}}

// type TagManager interface {{{

// To do any shutdown work a TagManager should be provided a proper context.Context.
type TagManager interface {
	// Lookup a tag id from its string name.
	Get(string) (uint64, error)

	// Reverse lookup a tag name from its id.
	Name(uint64) (string, error)
} // }}}

// type IDManager interface {{{

// Maps between hashes and uint64 (IDs).
type IDManager interface {
	// Get an ID for the specified file hash
	GetID(string) (uint64, error)

	// Gets the hash mapping to the specified ID.
	GetHash(uint64) (string, error)
} // }}}

// type Profile struct {{{

// This is the final loaded profile with all the processing completed.
//
// The display and tag profiles have been merged in and processed.
type Profile struct {
	// The name of the given profile.
	Profile string

	// For our display output.
	Width    int
	Height   int
	MaxDepth int

	RandomLayout bool

	// The number of images to have waiting for clients at any given time.
	//
	// This defaults to 2 if not set.
	Queue int

	// The tag rules for this profile
	Require tags.Tags
	Exclude tags.Tags
	Weights tags.TagWeights
} // }}}
