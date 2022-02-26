package image

import (
	"image"
	"testing"
)


type fitPointTest struct {
	FitTo    image.Point
	ImgSize  image.Point
	Expected image.Point
	Ratio    float64
	Enlarge  bool
}

func TestFitPoint(t *testing.T) {
	// Our tests to run.
	tests := []fitPointTest{
		{image.Point{1024, 1024}, image.Point{123, 2321}, image.Point{54, 1024}, 0, true},
		{image.Point{1600, 1200}, image.Point{2076, 1800}, image.Point{1384, 1200}, 0, true},
		{image.Point{3200, 1800}, image.Point{1800, 1273}, image.Point{2545, 1800}, 0, true},
		{image.Point{2960, 1800}, image.Point{629, 1367}, image.Point{828, 1800}, 0, true},
		{image.Point{2960, 1800}, image.Point{629, 1367}, image.Point{629, 1367}, 0, false},
		{image.Point{1440, 1560}, image.Point{1318, 862}, image.Point{1440, 942}, 0, true},
	}

	for _, test := range tests {
		res, ratio := Fit(test.ImgSize, test.FitTo, test.Enlarge)
		if test.Ratio != 0 && test.Ratio != ratio {
			t.Logf("Test: %#v", test)
			t.Fatalf("Expected Ratio %f != Got %f", test.Ratio, ratio)
		}

		if res != test.Expected {
			t.Logf("Test: %#v", test)
			t.Fatalf("Expected %v != Got %v", test.Expected, res)
		}
	}
}
