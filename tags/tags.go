package tags

import (
	"fmt"
	"sort"
)

// func Tags.Copy {{{

// Creates a copy of the input tags sized to only the amount of tags in t.
//
// This does not call Fix() on the returned tags, the caller should do that first.
func (t Tags) Copy() Tags {
	nTags := make(Tags, len(t))

	for i, v := range t {
		nTags[i] = v
	}

	return nTags
} // }}}

// func Tags.Fix {{{

// This fixes the tags to be used for comparing and matching between them.
//
// It -
//
// - Removes duplicate tags
// - Sorts the tags
func (t Tags) Fix() Tags {
	// No tags? Nothing to do.
	if len(t) == 0 {
		return t
	}

	// If length is only 1, no need to sort or check for dupliates.
	if len(t) == 1 {
		return t
	}

	// Now we sort.
	//
	// Sorting makes it far easier to check for duplicates.
	t.Sort()

	// Lets check for duplicates.
	for i := 1; i < len(t); i++ {
		// Is this tag the same as the previous tag?
		if t[i-1] == t[i] {
			// Yep, so we need to remove it.
			// Lets move everything after it forward, then resize ourself.
			for j := i; j < len(t); j++ {
				t[j-1] = t[j]
			}

			// Now we copy the array back to ourself, cutting off the last value
			t = t[:len(t)-1]

			// So i stays at the same place, in case there are multiple duplicates in a row.
			i--
		}
	}

	return t
} // }}}

// func Tags.Equal {{{

// Returns true if both Tags contain the exact same tags.
//
// Note that both must have be run through Fix() already, otherwise it could return
// the incorrect result.
//
// Note that if both tags are empty, this also means they are equal.
func (t Tags) Equal(r Tags) bool {
	// Different lengths?
	if len(t) != len(r) {
		return false
	}

	// Is that equal length 0?
	if len(t) == 0 {
		return true
	}

	for i := 0; i < len(t); i++ {
		if t[i] != r[i] {
			return false
		}
	}

	return true
} // }}}

// func Tags.Contains {{{

// Returns true if this Tags contains 1 or more tags from the provided comparision Tags.
func (t Tags) Contains(r Tags) bool {
	// Basic sanity - If either one is empty, then neither can match.
	// They need to have at least 1 tag in common to match.
	if len(t) == 0 || len(r) == 0 {
		// One is empty, can not contain anything from the other.
		return false
	}

	// We are going to be comparing the two tags left to right.
	//
	// We start at the first value of each then move forward.
	lftLoc := 0
	rgtLoc := 0

	// Now we start going left to right, through the provied tags moving our
	// location forward each time after a comparision between the given two locations.
	for {
		// If either location goes over our lengths then the loop is done.
		if lftLoc >= len(t) || rgtLoc >= len(r) {
			break
		}

		// Is the left greater then the right?
		if t[lftLoc] > r[rgtLoc] {
			// Left is greater then right, so move right forward so its either equal itself or greater.
			rgtLoc++
			continue
		}

		// Is the right greater then the left?
		if r[rgtLoc] > t[lftLoc] {
			// Right is greater then left, so move left forward until its equal itself or greater.
			lftLoc++
			continue
		}

		// If we are here, both sides are now equal.
		return true
	}

	// No match was found before one of the two found their end.
	return false
} // }}}

// func Tags.Combine {{{

// Combines tags from two tag list and returns the combined result.
//
// Note this tries to combine the result into t, so this may (or may not) modifed t.
//
// Best result call with -
//
//  t = t.Combine(other)
//
// Similar to the way append() works.
//
// No need to run Fix() on the result.
func (t Tags) Combine(r Tags) Tags {
	var newTags Tags

	// This logic is similar to Contains(), it runs through both lists from left to right, except that
	// each missing tag it adds that to a temporary array before finally adding the missing ones to the input Tags.
	//
	// Now if t has no tags, just return add.
	if len(t) == 0 {
		return r
	}

	// And if add has no tags, just return t.
	if len(r) == 0 {
		return t
	}

	// We are going to be comparing the two tags left to right.
	//
	// We start at the first value of each then move forward.
	lftLoc := 0
	rgtLoc := 0

	// Now we start going left to right, through the provied tags moving our
	// location forward each time after a comparision between the given two locations.
	for {
		// If either location goes over our lengths then the loop is done.
		if lftLoc >= len(t) || rgtLoc >= len(r) {
			break
		}

		// Is the left greater then the right?
		if t[lftLoc] > r[rgtLoc] {
			// Left is greater then right, as we are adding tags to the left we just skip adding this to the array.
			newTags = append(newTags, r[rgtLoc])
			rgtLoc++
			continue
		}

		// Is the right greater then the left?
		if r[rgtLoc] > t[lftLoc] {
			// Right is greater then left, so we need to add this tag to the array
			lftLoc++
			continue
		}

		// If we are here, both sides are now equal.
		lftLoc++
		rgtLoc++
	}

	// Does right have any additional tags that were not seen?
	if len(r) > rgtLoc {
		newTags = append(newTags, r[rgtLoc:]...)
	}

	// Now if any new tags were found, add them.
	if len(newTags) > 0 {
		t = append(t, newTags...)
		t = t.Fix()
	}

	// Return the new tags
	return t
} // }}}

// func Tags.Add {{{

// Adds the given Tag to the tag list.
//
// If the tag is already in the list it simply returns the same list.
//
// If not it will append to the list and handle Fix() for you.
func (t Tags) Add(toAdd uint64) Tags {

	// Ensure the ID is actually valid
	if toAdd == 0 {
		return t
	}

	// If we already have the tag just return.
	if t.Has(toAdd) {
		return t
	}

	// Nope, do not have it.
	// So append it.
	t = append(t, toAdd)

	// Now sort the list.
	//
	// We do not need to run Fix() as we already know this is not a duplicate.
	t.Sort()

	return t
} // }}}

// func Tags.Has {{{

// Returns true if this Tags contains the provided tag.
func (t Tags) Has(want uint64) bool {
	// Basic sanity.
	if want == 0 {
		// Invalid Tag
		return false
	}

	// Now we start going left to right, through the provied tags moving our
	// location forward each time after a comparision between the given two locations.
	for i := 0; i < len(t); i++ {
		// Is this what they are looking for?
		if t[i] == want {
			return true
		}

		// Is the value already past what we are looking at?
		// IE, they want "ben" and we are at "sam", alphabetically if
		// we had "ben" we would have seen it by now.
		if t[i] > want {
			return false
		}
	}

	// No match was found before one of the two found their end.
	return false
} // }}}

func (tw TagWeights) Len() int           { return len(tw) }
func (tw TagWeights) Less(i, j int) bool { return tw[i].Tag < tw[j].Tag }
func (tw TagWeights) Swap(i, j int)      { tw[i], tw[j] = tw[j], tw[i] }
func (tw TagWeights) Sort()              { sort.Sort(tw) }

func (trt trTags) Len() int           { return len(trt) }
func (trt trTags) Less(i, j int) bool { return trt[i].tag < trt[j].tag }
func (trt trTags) Swap(i, j int)      { trt[i], trt[j] = trt[j], trt[i] }
func (trt trTags) Sort()              { sort.Sort(trt) }

func (t Tags) Len() int           { return len(t) }
func (t Tags) Less(i, j int) bool { return t[i] < t[j] }
func (t Tags) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Tags) Sort()              { sort.Sort(t) }

// func TagWeights.Fix {{{

// Does much the same as Tags.Fix.
//
// When it finds duplicate tags though it combines the weights of both.
//
// So if you have two tags name "hello", with a weight of 4 and the second of 2, you'd end up with
// one tag with a combined weight of 6.
func (tw TagWeights) Fix() TagWeights {
	// No weights? Nothing to do.
	if len(tw) == 0 {
		return tw
	}

	// If length is only 1, no need to sort or check for dupliates.
	if len(tw) == 1 {
		return tw
	}

	// Now we sort.
	//
	// Sorting makes it far easier to check for duplicates.
	tw.Sort()

	// Lets check for duplicates.
	for i := 1; i < len(tw); i++ {
		// Is this tag the same as the previous tag?
		if tw[i-1].Tag == tw[i].Tag {
			// Combine the weights first, then move forward.
			tw[i].Weight += tw[i-1].Weight

			// Yep, so we need to remove it.
			// Lets move everything after it forward, then resize ourself.
			for j := i; j < len(tw); j++ {
				tw[j-1].Tag = tw[j].Tag
				tw[j-1].Weight = tw[j].Weight
			}

			// Now we copy the array back to ourself, cutting off the last value
			tw = tw[:len(tw)-1]

			// So i stays at the same place, in case there are multiple duplicates in a row.
			i--
		}
	}

	return tw
} // }}}

// func TagWeights.GetWeight {{{

// Returns the total weight of the provided tags.
func (tw TagWeights) GetWeight(t Tags) int {
	// Your basic sanity checking first.
	//
	// If either tags or tagweight is empty, then the weight can only be 0.
	if len(tw) == 0 || len(t) == 0 {
		return 0
	}

	// This is much the same as Tags.Contains.
	//
	// We are moving left to right, so we start at 0 for both.
	twLoc := 0
	tLoc := 0

	// Weight starts at 0.
	weight := 0

	// Now we start going left to right, through the provied lists moving our
	// location forward each time after a comparision between the given two locations.
	for {
		// If either location goes over our lengths then the loop is done.
		if twLoc >= len(tw) || tLoc >= len(t) {
			break
		}

		// Is the tw tag greater then the t tag?
		if tw[twLoc].Tag > t[tLoc] {
			// Yep, so t forward and continue.
			tLoc++
			continue
		}

		// Reverse of the above, t greater then tw?
		if t[tLoc] > tw[twLoc].Tag {
			// Yep, to move tw forward.
			twLoc++
			continue
		}

		// If we are here, both sides are now equal, so the tag matches.
		// Add to the weight and continue.
		weight += tw[twLoc].Weight

		// Now move both our locations forward one.
		twLoc++
		tLoc++
	}

	// We've gone through all the tags, so return the total weight calculated.
	return weight
} // }}}

// func TagWeights.Equal {{{

// Returns true if both Tags contain the exact same tags.
//
// Note that both must have be run through Fix() already, otherwise it could return
// the incorrect result.
//
// Note that if both tags are empty, this also means they are equal.
func (tw TagWeights) Equal(r TagWeights) bool {
	// Any of the tags empty?
	if len(tw) == 0 || len(r) == 0 {
		// Both of them empty?
		if len(tw) == len(r) {
			return true
		}

		return false
	}

	// If the lengths are different, then they can not match.
	if len(tw) != len(r) {
		return false
	}

	for i := 0; i < len(tw); i++ {
		if tw[i].Tag != r[i].Tag || tw[i].Weight != r[i].Weight {
			return false
		}
	}

	return true
} // }}}

// func MakeTagRule {{{

// Creates a TagRule from a given list of Any, All and None tags.
//
// Can return an error if the same tag is in multiple lists or no
// tags to match with.
func MakeTagRule(give uint64, any, all, none Tags) (TagRule, error) {
	var hasAny, hasAll, hasNone bool

	trt := trTags{}

	for _, tn := range any {
		hasAny = true
		trt = append(trt, trTag{tag: tn, flag: trfAny})
	}

	for _, tn := range all {
		hasAll = true
		trt = append(trt, trTag{tag: tn, flag: trfAll})
	}

	for _, tn := range none {
		hasNone = true
		trt = append(trt, trTag{tag: tn, flag: trfNone})
	}

	// Ok, sort the tags.
	trt.Sort()

	if len(trt) == 0 {
		return TagRule{}, fmt.Errorf("No tags in TagRule %d", give)
	}

	// Lets check for duplicates.
	//
	// If we get a duplicate that is a different flag, then the only way to handle it is to return an error and let the user fix the configuration.
	for i := 1; i < len(trt); i++ {
		// Is this tag the same as the previous tag?
		if trt[i-1].tag == trt[i].tag {
			return TagRule{}, fmt.Errorf("Duplicate tag %d found in TagRule %d", trt[i].tag, give)
		}
	}

	return TagRule{
		Tag:     give,
		trTags:  trt,
		hasAny:  hasAny,
		hasAll:  hasAll,
		hasNone: hasNone,
	}, nil
} // }}}

// func TagRules.Equal {{{

// Returns true if both TagRules are exactly the same.
func (trs TagRules) Equal(co TagRules) bool {
	// Different lengths?
	if len(trs) != len(co) {
		return false
	}

	// Is that equal length 0?
	if len(trs) == 0 {
		return true
	}

	// Now iterate the rules and check directly.
	for i := 0; i < len(trs); i++ {
		lft := trs[i]
		rht := co[i]

		if lft.Tag != rht.Tag {
			return false
		}

		if lft.hasAny != rht.hasAny || lft.hasAll != rht.hasAll || lft.hasNone != rht.hasNone {
			return false
		}

		// Now the individual tags
		if len(lft.trTags) != len(rht.trTags) {
			return false
		}

		for j := 0; j < len(lft.trTags); j++ {
			if lft.trTags[j] != rht.trTags[j] {
				return false
			}
		}
	}

	// They pass all the checks, they are equal.
	return true
} // }}}

// func TagRules.Combine {{{

// This combines the two TagRules.
//
// Note this tries to combine the result into trs, so this will modify trs.
//
// Best result call with -
//
//  trs = trs.Combine(other)
//
// Similar to the way append() works, the rules from other are appended after the original trs rules.
func (trs TagRules) Combine(co TagRules) TagRules {
	// This is actually a fairly simple append here.
	for _, otr := range co {
		trs = append(trs, otr)
	}

	return trs
} // }}}

// func TagRules.Apply {{{

// This runs through a TagRules slice and applies all the rules in sequence, changing the input Tags as required by the rules
// and returning the new Tags.
func (trs TagRules) Apply(t Tags) Tags {
	for _, tr := range trs {
		if tr.Give(t) {
			t = t.Add(tr.Tag)
		}
	}

	return t
} // }}}

// func TagRule.Give {{{

// Returns true if the TagRule applied and should be given or not.
func (tr *TagRule) Give(t Tags) bool {
	var hasAny, onlyAny bool

	// Just basic sanity.
	//
	// We (tr) need at least 1 tag (typically an any or all) to give this tag.
	//
	// Note that the tags given (t) *can* be empty.
	//
	// As it is possible to have a TagRule with only None, which would match
	// if the provided list of Tags doesn't have any.
	if len(tr.trTags) < 1 {
		return false
	}

	// Handle if t is empty.
	if len(t) < 1 {
		// Some quick matching.
		//
		// If we have Any or All tags then we can not match.
		if tr.hasAny || tr.hasAll {
			return false
		}

		// We have no Any or All tags, so then any tags we have should natrually be None.
		//
		// But you know, sanity.
		if tr.hasNone {
			return true
		}

		// We should only ever get here if there is a 4th or more flags?
		return false
	}

	trLoc := 0
	tLoc := 0

	trt := tr.trTags

	// Is this rule only Any tags?
	//
	// This speeds up matching for long Any tags, as we can just return at the first Any tag and not bother checking anything else.
	if tr.hasAny && !tr.hasAll && !tr.hasNone {
		onlyAny = true
	}

	for {
		if trLoc >= len(trt) || tLoc >= len(t) {
			break
		}

		if trt[trLoc].tag > t[tLoc] {
			tLoc++
			continue
		}

		if t[tLoc] > trt[trLoc].tag {
			// Skipping a rule tag.
			// If its a trfAll then can not match.
			if trt[trLoc].flag == trfAll {
				return false
			}

			trLoc++
			continue
		}

		// Both sides are equal here, so what is this match?
		switch trt[trLoc].flag {
		case trfAny:
			// This have only Any tags?
			if onlyAny {
				// Yep, so any match is allowed.
				return true
			}

			// They have at least 1 Any tag, so flag it.
			hasAny = true
		case trfAll:
		case trfNone:
			// Should not have this tag, so not possible to match.
			return false
		}

		// Move both forward.
		trLoc++
		tLoc++
	}

	// Did we match any Any?
	if tr.hasAny && hasAny {
		return true
	}

	// The All tags have to match other we return when we see them.
	//
	// So if hasAll is set here, we know we saw them.
	if tr.hasAll {
		return true
	}

	// Nothing matched.
	return false
} // }}}
