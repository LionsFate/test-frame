package image

import (
	"github.com/anthonynsimon/bild/transform"
	"github.com/disintegration/imaging"
	"github.com/nfnt/resize"
	"testing"
)

// func TestResizeN {{{

func TestResizeN(t *testing.T) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		t.Fatal(err)
	}

	// Now resize it using nfnt.
	_ = resize.Resize(uint(2068), uint(1440), img, resize.Lanczos3)
} // }}}

// func TestResizeB {{{

func TestResizeB(t *testing.T) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		t.Fatal(err)
	}

	// Now resize it using bild
	_ = transform.Resize(img, 2068, 1440, transform.Lanczos)
} // }}}

// func TestResizeI {{{

func TestResizeI(t *testing.T) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		t.Fatal(err)
	}

	// Now resize it using bild
	_ = imaging.Resize(img, 2068, 1140, imaging.Lanczos)
} // }}}

// func BenchmarkResizeNL2 {{{

func BenchmarkResizeNL2(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = resize.Resize(uint(2068), uint(1440), img, resize.Lanczos2)
	}
} // }}}

// func BenchmarkResizeNL3 {{{

func BenchmarkResizeNL3(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = resize.Resize(uint(2068), uint(1440), img, resize.Lanczos3)
	}
} // }}}

// func BenchmarkResizeNN {{{

func BenchmarkResizeNN(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = resize.Resize(uint(2068), uint(1440), img, resize.NearestNeighbor)
	}
} // }}}

// func BenchmarkResizeNM {{{

func BenchmarkResizeNM(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = resize.Resize(uint(2068), uint(1440), img, resize.MitchellNetravali)
	}
} // }}}

// func BenchmarkResizeBL {{{

func BenchmarkResizeBL(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = transform.Resize(img, 2068, 1440, transform.Lanczos)
	}
} // }}}

// func BenchmarkResizeBN {{{

func BenchmarkResizeBN(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = transform.Resize(img, 2068, 1440, transform.NearestNeighbor)
	}
} // }}}

// func BenchmarkResizeBM {{{

func BenchmarkResizeBM(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = transform.Resize(img, 2068, 1440, transform.MitchellNetravali)
	}
} // }}}

// func BenchmarkResizeIL {{{

func BenchmarkResizeIL(b *testing.B) {
	// Original - 2560,1782, new 2068,1440
	testImage := "/Syno6/Media/Family Photos/Roberta/img061.jpg"

	img, err := Open(testImage)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = imaging.Resize(img, 2068, 1140, imaging.Lanczos)
	}
} // }}}
