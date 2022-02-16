package types

import (
	"errors"
	"frame/tags"
	"image"
	"io"
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

// type CacheManager interface {{{

// Used to handle all our image caching needs.
//
// Because we wanted to make CacheManager able to store and handles images any way
// it wants to, we avoided using image.Image as a type. Rather CacheManager itself
// will read and decode the image when CacheImage() is called, and return a raw
// io.ReadCloser to the caller can likewise decode the image using whatever package
// they want, be it image.Image or any other.
//
// During developement I tried many different image packages and they all performed
// very differently, especially considering I wanted to run this on x86, x86-64, ARM
// and ARM64.
//
// Pretty much the best way to handle all this is just allowing the callers
// to provied raw io types back and fourth.
type CacheManager interface {
	// Given a raw io.Reader to an image of either JPEG, PNG, GIF or WebP
	// it will hash to the cache using whatever hash method its configured for,
	// cache it and then return the ID provided by IDManager.
	//
	// Note that since this is using an io.Reader, its up to the caller to
	// call and Close() after it returns if needed (like os.File).
	//
	// Only the 4 types above are supported, any other types please use
	// CacheImage() instead.
	CacheImageRaw(io.Reader) (uint64, error)

	// Same as CacheImage but with a provieded image.Image, useful for file
	// types that are not otherwise supported.
	CacheImage(image.Image) (uint64, error)

	// Given an ID (uint64) originally provided by CacheImage() this will return
	// an image.Image
	//
	// The image will returned will be resized (shrunk) to be no larger then
	// the provided image.Point.
	//
	// The image will *not* be resized larger then it actually is,
	// unless the 3rd option (bool) is true.
	//
	// Requesting that the image is enlarged can result in pixelated images
	// so it must be specified.
	//
	// If the provided image.Point is 0x0 then the original size will
	// be returned.
	LoadImage(uint64, image.Point, bool) (image.Image, error)
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
