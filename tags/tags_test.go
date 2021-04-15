package tags

import (
	"testing"
)

// func TestContains {{{

func TestContains(t *testing.T) {
	tLeft := Tags{4, 2, 10, 21, 24, 3}
	tRightA := Tags{5, 9, 1, 6}
	tRightB := Tags{31, 7, 18, 33, 9, 5}
	tRightC := Tags{30, 22, 18, 2}

	tLeft = tLeft.Fix()
	tRightA = tRightA.Fix()
	tRightB = tRightB.Fix()
	tRightC = tRightC.Fix()

	if tLeft.Contains(tRightA) {
		t.Fatal("tLeft contains A?")
	}

	if tRightA.Contains(tLeft) {
		t.Fatal("A contains tLeft?")
	}

	if tLeft.Contains(tRightB) {
		t.Fatal("tLeft contains B?")
	}

	if tRightB.Contains(tLeft) {
		t.Fatal("B contains tLeft?")
	}

	if !tLeft.Contains(tRightC) {
		t.Fatal("tLeft does not contain C?")
	}

	if !tRightC.Contains(tLeft) {
		t.Fatal("C does not contain tLeft?")
	}
} // }}}

// func TestHas {{{

func TestHas(t *testing.T) {
	tgs := Tags{4, 2, 8, 20, 30, 3}

	tgs = tgs.Fix()

	if tgs.Has(5) {
		t.Fatal("5")
	}

	if !tgs.Has(8) {
		t.Fatal("8")
	}

	if tgs.Has(22) {
		t.Fatal("22")
	}

	if !tgs.Has(30) {
		t.Fatal("30")
	}
} // }}}

// func TestEqual {{{

func TestEqual(t *testing.T) {
	tLeft := Tags{1, 2, 3, 4}
	tEqa1 := Tags{3, 2, 4, 1}
	tEqa2 := Tags{1, 5, 4, 3}
	tEqa3 := Tags{1, 4, 3, 3}

	tLeft = tLeft.Fix()
	tEqa1 = tEqa1.Fix()
	tEqa2 = tEqa2.Fix()
	tEqa3 = tEqa3.Fix()

	if !tLeft.Equal(tEqa1) {
		t.Fatal("Left != Eqa1")
	}

	if tLeft.Equal(tEqa2) {
		t.Fatal("Left == Eqa2")
	}

	if tLeft.Equal(tEqa3) {
		t.Fatal("Left == Eqa3")
	}
} // }}}

// func TestTagRules {{{

func TestTagRules(t *testing.T) {
	ttm := NewTestTM()
	mtr := func(ctr ConfTagRules) TagRules {
		trs, err := ConfMakeTagRules(ctr, ttm)
		if err != nil {
			t.Fatal(err)
		}

		return trs
	}
	stt := func(in []string) Tags {
		tgs, err := StringsToTags(in, ttm)
		if err != nil {
			t.Fatal(err)
		}

		return tgs
	}
	get := func(in string) uint64 {
		tag, err := ttm.Get(in)
		if err != nil {
			t.Fatal(err)
		}

		return tag
	}

	// Just a random long list of tags really.
	tgs := stt([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "two"})
	trs := mtr(ConfTagRules{
		ConfTagRule{
			Tag: "tr_one",
			Any:  []string{ "one", "two", "five", "seven" },
		},
		ConfTagRule{
			Tag: "tr_two",
			None: []string{ "tr_one" },
		},
	})

	// Should just have tr_one
	tgs = trs.Apply(tgs)
	if !tgs.Has(get("tr_one")) {
		t.Fatalf("Missing tr_one")
	}
	
} // }}}

// func TestTagRuleA {{{

func TestTagRuleA(t *testing.T) {
	ttm := NewTestTM()
	stt := func(in []string) Tags {
		tgs, err := StringsToTags(in, ttm)
		if err != nil {
			t.Fatal(err)
		}

		return tgs
	}

	// Lets say a basic rule, immediate family only.
	trc := &ConfTagRule{
		Tag: "immediate",
		Any:  []string{"brother", "sister", "mother", "father"},
		None: []string{"uncle", "aunt"},
	}

	tr, err := ConfMakeTagRule(trc, ttm)
	if err != nil {
		t.Fatalf("ConfMakeTagRule(immediate): %s", err)
	}

	immBM := stt([]string{"brother", "mother", "dog"})
	if !tr.Give(immBM) {
		t.Fatalf("%#v.Give(%#v) != true", tr, immBM)
	}

	immBMS := stt([]string{"sister", "brother", "cat", "mother"})
	if !tr.Give(immBMS) {
		t.Fatalf("%#v.Give(%#v) != true", tr, immBMS)
	}

	immBU := stt([]string{"uncle", "brother"})
	if tr.Give(immBU) {
		t.Logf("tr = %#v", tr)
		t.Logf("immBU = %#v", immBU)
		t.Fatal("tr.Give(immBU) == true")
	}
} // }}}

// func TestFix {{{

func TestFix(t *testing.T) {
	tOrig := Tags{3, 3, 1, 2, 1, 3}

	// This is the above fixed
	tFixed := Tags{1, 2, 3}

	tOrig = tOrig.Fix()

	if len(tOrig) != len(tFixed) {
		t.Logf("tOrig = %#v", tOrig)
		t.Fatalf("sizes tOrig (%d) != tFixed (%d)", len(tOrig), len(tFixed))
	}

	for i, _ := range tOrig {
		if tOrig[i] != tFixed[i] {
			t.Fatalf("tOrig %d (%d) != tFixed %d (%d)", tOrig[i], i, tFixed[i], i)
		}
	}
} // }}}

// func TestCombine {{{

func TestCombine(t *testing.T) {
	tOne := Tags{1, 2, 3, 4, 5}
	tTwo := Tags{3, 2, 5, 7, 9}
	tEqa := Tags{1, 2, 3, 4, 5, 7, 9}

	tOne = tOne.Fix()
	tTwo = tTwo.Fix()

	tOne = tOne.Combine(tTwo)

	if !tOne.Equal(tEqa) {
		t.Fatal("Not equal")
	}

	tA := Tags{}
	tB := Tags{1, 2, 3}

	tA = tA.Combine(tB)

	if !tA.Equal(tB) {
		t.Fatalf("tA(%#v) != tEqa(%#v) A", tA, tEqa)
	}

	tA = Tags{1, 3, 5, 7}
	tB = Tags{2, 4, 6, 8}
	tEqa = Tags{1, 2, 3, 4, 5, 6, 7, 8}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) B", tA, tEqa)
	}

	tA = Tags{1, 2, 3}
	tB = Tags{}
	tEqa = Tags{1, 2, 3}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) C", tA, tEqa)
	}

	tA = Tags{1, 2, 7, 8, 9, 10, 11, 12}
	tB = Tags{1, 2, 3, 4, 5}
	tEqa = Tags{1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) D", tA, tEqa)
	}

	tA = Tags{1, 2, 3, 4, 5}
	tB = Tags{1, 2, 7, 8, 9, 10, 11, 12}
	tEqa = Tags{1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) E", tA, tEqa)
	}

	tA = Tags{10, 11, 12, 13}
	tB = Tags{20, 21, 22, 23}
	tEqa = Tags{10, 11, 12, 13, 20, 21, 22, 23}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) F", tA, tEqa)
	}

	tA = Tags{20, 21, 22, 23}
	tB = Tags{10, 11, 12, 13}
	tEqa = Tags{10, 11, 12, 13, 20, 21, 22, 23}

	tA = tA.Combine(tB)

	if !tA.Equal(tEqa) {
		t.Fatalf("tA(%#v) != tEqa(%#v) G", tA, tEqa)
	}
} // }}}

// func BenchmarkEqual4a {{{

func BenchmarkEqual4a(b *testing.B) {
	tLeft := Tags{1, 2, 3, 4}
	tEqa1 := Tags{3, 2, 4, 1}

	tLeft = tLeft.Fix()
	tEqa1 = tEqa1.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tLeft.Equal(tEqa1) {
			b.Fatal("Equal")
		}
	}
} // }}}

// func BenchmarkEqual4b {{{

func BenchmarkEqual4b(b *testing.B) {
	tLeft := Tags{1, 2, 3, 4}
	tEqa1 := Tags{3, 4, 1}

	tLeft = tLeft.Fix()
	tEqa1 = tEqa1.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if tLeft.Equal(tEqa1) {
			b.Fatal("Equal")
		}
	}
} // }}}

// func BenchmarkContains2a {{{

func BenchmarkContains2a(b *testing.B) {
	tLeft := Tags{4, 8, 4, 5, 2}
	tRight := Tags{1, 5}

	tLeft = tLeft.Fix()
	tRight = tRight.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tLeft.Contains(tRight) {
			b.Fatal("Contains")
		}
	}
} // }}}

// func BenchmarkContains2b {{{

func BenchmarkContains2b(b *testing.B) {
	tLeft := Tags{2, 4, 6, 8, 5}
	tRight := Tags{1, 2}

	tLeft = tLeft.Fix()
	tRight = tRight.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tLeft.Contains(tRight) {
			b.Fatal("Contains")
		}
	}
} // }}}

/*

// func BenchmarkContains2c {{{

func BenchmarkContains2c(b *testing.B) {
	tLeft := Tags{"man", "woman", "dog", "cat", "feline", "mutt"}
	tRight := Tags{"one", "man"}

	tLeft = tLeft.Fix()
	tRight = tRight.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tRight.Contains(tLeft) {
			b.Fatal("Contains")
		}
	}
} // }}}

// func BenchmarkContains3 {{{

func BenchmarkContains3(b *testing.B) {
	tLeft := Tags{"man", "woman", "dog", "cat", "feline", "mutt"}
	tRight := Tags{"one", "cat", "three"}

	tLeft = tLeft.Fix()
	tRight = tRight.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tLeft.Contains(tRight) {
			b.Fatal("Contains")
		}
	}
} // }}}

// func BenchmarkContains5 {{{

func BenchmarkContains5(b *testing.B) {
	tLeft := Tags{"man", "woman", "dog", "cat", "feline", "mutt"}
	tRight := Tags{"one", "two", "three", "four", "muTT"}

	tLeft = tLeft.Fix()
	tRight = tRight.Fix()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !tLeft.Contains(tRight) {
			b.Fatal("Contains")
		}
	}
} // }}}

// func TestWeightsSort {{{

func TestWeightsSort(t *testing.T) {
	twOrig := TagWeights{
		TagWeight{"two", 2},
		TagWeight{"one", 1},
		TagWeight{"four", 8},
		TagWeight{"six", 6},
	}

	twSorted := TagWeights{
		TagWeight{"four", 8},
		TagWeight{"one", 1},
		TagWeight{"six", 6},
		TagWeight{"two", 2},
	}

	twOrig.Sort()

	if twOrig.Len() != twSorted.Len() {
		t.Logf("twOrig = %#v", twOrig)
		t.Logf("twSorted = %#v", twSorted)
		t.Fatal("Lengths")
	}

	for i, _ := range twOrig {
		if twOrig[i].Tag != twSorted[i].Tag || twOrig[i].Weight != twSorted[i].Weight {
			t.Logf("twOrig = %#v", twOrig)
			t.Logf("twSorted = %#v", twSorted)
			t.Fatalf("Value at %d", i)
		}
	}

} /// }}}

// func TestWeightsFix {{{

func TestWeightsFix(t *testing.T) {
	twOrig := TagWeights{
		TagWeight{"  FOUR  ", 1},
		TagWeight{"two", 1},
		TagWeight{"one", 1},
		TagWeight{"four", 2},
		TagWeight{"two", 1},
		TagWeight{"  fOUr", 1},
		TagWeight{"six", 6},
	}

	twFixed := TagWeights{
		TagWeight{"four", 4},
		TagWeight{"one", 1},
		TagWeight{"six", 6},
		TagWeight{"two", 2},
	}

	twOrig = twOrig.Fix()

	if twOrig.Len() != twFixed.Len() {
		t.Logf("twOrig = %#v", twOrig)
		t.Logf("twFixed = %#v", twFixed)
		t.Fatal("Lengths")
	}

	for i, _ := range twOrig {
		if twOrig[i].Tag != twFixed[i].Tag || twOrig[i].Weight != twFixed[i].Weight {
			t.Logf("twOrig = %#v", twOrig)
			t.Logf("twFixed = %#v", twFixed)
			t.Fatalf("Value at %d", i)
		}
	}
} // }}}

// func TestGetWeight {{{

func TestGetWeight(t *testing.T) {
	tw := TagWeights{
		TagWeight{"one", 1},
		TagWeight{"two", 2},
		TagWeight{"three", 3},
		TagWeight{"four", 4},
		TagWeight{"five", 5},
		TagWeight{"six", 6},

		// And negative weights, because why not?
		TagWeight{"minone", -1},
	}

	tw = tw.Fix()

	ta := Tags{"one", "two", "four", "minone"}
	ta.Fix()

	// one + two + four + minone = 6
	if tw.GetWeight(ta) != 6 {
		t.Logf("tw = %#v", tw)
		t.Logf("t = %#v", ta)
		t.Fatalf("weight (%d) != 6", tw.GetWeight(ta))
	}
} /// }}}

*/
