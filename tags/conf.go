package tags

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// This contains all the functions and types needed to load tags from a YAML/JSON configuration file.

// type ConfTagRule struct {{{

// Tag rules allow you to define rules that automatically add other tags.
//
// Lets say we have a list of tags for a photo, and you want to create simpler tags for tag matching.
//
// A simple tag that says "This has 1 or more of my siblings in it" -
//
//   tagrule:
//     tag: siblings
//     any:
//       - brother 1
//       - brother 2
//       - sister 1
//       - sister 2
//
//
// Now you want a separate tag only if all your siblings are in the photo (group photos for example are easier to find this way) -
//
//   tagrule:
//     tag: sibling_group
//     all:
//       - brother 1
//       - brother 2
//       - sister 1
//       - sister 2
//
// And a tag that ensures the group only is that group, removing all possible spouces -
//
//   tagrule:
//     tag: sbling_group_only
//     none: [ brother_1_spouse, brother_2_spouse, sister_1_spouse, sister_2_spouse ]
//
// Tag rules support any combination of "any", "all" and/or "none" (though you must have 1 of them for a tag rule to be valid)
//
//  - "any" tag means you need at least 1 of the tags within to match.
//  - "all" means you need all of the tags within to match.
//  - "none" means you can not have any of the tags within to match.
//
// Tag rules can rely on tags given by other tag rules as well, but in this situation the order of the tag rules is important.
//
// Tag rules in ConfTagRules are run in order so that earlier rules can give tags that later rules can use themselves.
//
// Multiple tag rules can give the same tag.
type ConfTagRule struct {
	Tag  string   `yaml:"tag" json:"tag"`
	Any  []string `yaml:"any" json:"any"`
	All  []string `yaml:"all" json:"all"`
	None []string `yaml:"none" json:"none"`
} // }}}

type ConfTagRules []ConfTagRule

type ConfTagWeights map[string]int

type TagManager interface {
	Get(string) (uint64, error)
}

// func ConfMakeTagWeights {{{

func ConfMakeTagWeights(ctw ConfTagWeights, tm TagManager) (TagWeights, error) {
	// Pre-allocate the space we expect we will need.
	tw := make(TagWeights, 0, len(ctw))

	for tag, weight := range ctw {
		id, err := tm.Get(tag)
		if err != nil {
			return tw, err
		}

		tw = append(tw, TagWeight{
			Tag:    id,
			Weight: weight,
		})
	}

	// Sort the TagWeights.
	sort.Slice(tw, func(i, j int) bool { return tw[i].Tag < tw[j].Tag })

	return tw, nil
} // }}}

// func ConfMakeTagRule {{{

func ConfMakeTagRule(ctr *ConfTagRule, tm TagManager) (TagRule, error) {
	var any, all, none Tags

	// Convert the name of the TagRule itself.
	gtag, err := tm.Get(ctr.Tag)
	if err != nil {
		return TagRule{}, err
	}

	// We need to convert all the string tags to integers first.
	if len(ctr.Any) > 0 {
		any = make(Tags, 0, len(ctr.Any))
		for _, str := range ctr.Any {
			tag, err := tm.Get(str)
			if err != nil {
				return TagRule{}, err
			}

			any = append(any, tag)
		}
	}

	if len(ctr.All) > 0 {
		all = make(Tags, 0, len(ctr.All))
		for _, str := range ctr.All {
			tag, err := tm.Get(str)
			if err != nil {
				return TagRule{}, err
			}

			all = append(all, tag)
		}
	}

	if len(ctr.None) > 0 {
		none = make(Tags, 0, len(ctr.None))
		for _, str := range ctr.None {
			tag, err := tm.Get(str)
			if err != nil {
				return TagRule{}, err
			}

			none = append(none, tag)
		}
	}

	tr, err := MakeTagRule(gtag, any, all, none)
	if err != nil {
		return TagRule{}, err
	}

	return tr, nil
} // }}}

// func ConfMakeTagRules {{{

func ConfMakeTagRules(ctr ConfTagRules, tm TagManager) (TagRules, error) {
	trs := make(TagRules, 0, len(ctr))

	for _, ctr := range ctr {
		tr, err := ConfMakeTagRule(&ctr, tm)
		if err != nil {
			return trs, err
		}

		trs = append(trs, tr)
	}

	return trs, nil
} // }}}

// func StringsToTags {{{

// Useful for loading []string types from configuration files and converting them to Tags.
//
// This also runs Fix() on the resulting Tags.
func StringsToTags(in []string, tm TagManager) (Tags, error) {
	if len(in) < 1 {
		// An empty list is still valid.
		return Tags{}, nil
	}

	out := make(Tags, 0, len(in))

	for _, str := range in {
		nt, err := tm.Get(str)
		if err != nil {
			return Tags{}, err
		}

		out = append(out, nt)
	}

	// Now fix the output
	out = out.Fix()

	return out, nil
} // }}}

type TestTM struct {
	tMut   sync.Mutex
	tags   map[string]uint64
	lastID uint64
}

// func NewTestTM {{{

// For testing - Creates a new in-memory TagManager.
//
// USE ONLY FOR TESTING.
func NewTestTM() *TestTM {
	return &TestTM{
		tags: make(map[string]uint64, 10),
	}
} // }}}

// func TestTM.Get {{{

// To make testing easier, an in-memory TagManager
func (tm *TestTM) Get(in string) (uint64, error) {
	tm.tMut.Lock()
	defer tm.tMut.Unlock()

	in = strings.ToLower(in)
	in = strings.TrimSpace(in)
	if in == "" {
		return 0, errors.New("Empty tag")
	}

	if val, ok := tm.tags[in]; ok {
		return val, nil
	}

	// Not found, so give it an ID and add it.
	tm.lastID++
	id := tm.lastID
	tm.tags[in] = tm.lastID

	return id, nil
} // }}}
