package cmanager

import (
	"github.com/rs/zerolog"
	"image"
	"os"
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
	// Create a minimal CManager needed for this test.
	cm := &CManager{
		l: zerolog.New(os.Stdout).With().Timestamp().Logger(),
	}

	// Our tests to run.
	tests := []fitPointTest{
		{image.Point{1024, 1024}, image.Point{123, 2321}, image.Point{54, 1024}, 0, true},
		{image.Point{1600, 1200}, image.Point{2076, 1800}, image.Point{1384, 1200}, 0, true},
		{image.Point{3200, 1800}, image.Point{1800, 1273}, image.Point{2545, 1800}, 0, true},
		{image.Point{2960, 1800}, image.Point{629, 1367}, image.Point{828, 1800}, 0, true},
		{image.Point{2960, 1800}, image.Point{629, 1367}, image.Point{629, 1367}, 0, false},
	}

	for _, test := range tests {
		res, ratio := cm.fitPoint(test.ImgSize, test.FitTo, test.Enlarge)
		if test.Ratio != 0 && test.Ratio != ratio {
			cm.l.Info().Interface("test", test).Send()
			t.Fatalf("Expected Ratio %f != Got %f", test.Ratio, ratio)
		}

		if res != test.Expected {
			cm.l.Info().Interface("test", test).Send()
			t.Fatalf("Expected %v != Got %v", test.Expected, res)
		}
	}
}
