package yconf

import (
	"github.com/rs/zerolog"
	"sync"
	"time"
)

type loaded struct {
	// Timestamp for the newest modified configuration file.
	newest time.Time

	// Previously loaded conf
	conf interface{}
}

// When loading from a YAML file you typically load values into a string, or other basic types.
// But you often need to convert those values into something else.
//
// This function takes the loaded YAML value and passes it in for converting to another type.
// This is done during loading and before any Merge() calls are made.
//
// A useful example of this is if you want a host:IP to load from a YAML file but you want that value to be
// a net.TCPAddr instead.
//
// You would create a Convert() function that accepts the string value as input
// and returns the net.TCPAddr value as its output, or an error if there was an issue converting.
//
// Convert() can also be used for validation on the configuration setting, since returning an error will
// result in parsing failing for the provided file, and the program would continue with any previously
// loaded configuration ignoring the changes until the next successful loading without errors.
type Convert func(interface{}) (interface{}, error)

// The Merge function handles merging of multiple configuration items of the same name when loading from
// multiple configuration files.
//
// If only using a single file this isn't needed, as it only runs if more then 1 file is found and loaded.
//
// Note that Merge() is called after any Convert() function.
//
// The 1st value passed in is always the previously merged one, the 2nd is the one from
// the current file.
//
// So in terms of if your trying to merge both into the same one, its best to merge into the first.
type Merge func(interface{}, interface{}) (interface{}, error)

// This function is passed in the old loaded interface{}, then the new loaded value and checks for changes.
// If anything changed then true is returned.
//
// Note this function is called after all Convert() and Merges() have already been called, and all files in
// the configuration directory have been run through.
//
// This allows Conf to be aware of any actual content changes within the files.
// So things like changed timestamps, whitespace, etc won't cause a notification to be sent.
type Changed func(interface{}, interface{}) bool

// Anytime the configuration files change, this function is called and the Conf is provided.
type Notify func()

// Empty() is the only non-option function, the others can be set or left empty.
type Callers struct {
	// Returns an empty type that the YAML/JSON will be parsed into directly.
	//
	// Use convert to change it to a better internal type.
	Empty   func() interface{}
	Convert Convert
	Merge   Merge
	Changed Changed
	Notify  Notify
}

type YConf struct {
	// Our log for everything Conf related
	l zerolog.Logger

	// Store the base configuration path.
	confPath string

	// Lets us know when we need to shutdown.
	bye chan struct{}

	// So we know the type we load into.
	ca Callers

	loMut sync.RWMutex
	lo    *loaded
}
