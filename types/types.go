package types

import (
	"errors"
	"frame/tags"
)

var ErrShutdown = errors.New("Shutdown")

// type TagManager interface {{{

// To do any shutdown work a TagManager should be provided a proper context.Context.
type TagManager interface {
	// Lookup a tag id from its string name.
	Get(string) (uint64, error)

	// Reverse lookup a tag name from its id.
	Name(uint64) (string, error)
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
