package tags

type Tags []uint64

type TagWeight struct {
	Tag    uint64
	Weight int
}

type TagWeights []TagWeight

// type trTag struct {{{

// Contains tags for use within a TagRule.
//
// Mainly these tags also have flags.
type trTag struct {
	tag uint64

	// Flag to specify specifically what type of match this tag
	// is to be used for.
	//
	// These are the trX constants, currently trAny, trAll and trNone.
	flag int
} // }}}

type trTags []trTag

// Tag rule constants used in TRTag if the rule should apply or not.
const (
	trfAny  = 1 << iota
	trfAll  = 1 << iota
	trfNone = 1 << iota
)

// type TagRule struct {{{

type TagRule struct {
	// The tag to give if this rule applies.
	Tag    uint64

	// The actual tags to match against to see if this rule applies or not.
	trTags trTags

	// Small bool flags that help us make decisions quicker when matching.
	hasAny  bool
	hasAll  bool
	hasNone bool
} // }}}

type TagRules []TagRule
