// Various image functions that our other packages share in common.
//
// Note that all functions here use image.NRGBA, as this is what the package we use mostly depends on for
// best performance.
//
// If switching away from "github.com/disintegration/imaging", this output may have to be changed.
//
// To make this easier, the ImageToPrefer() function exists.
//
// This changes a image.Image to whatever internally we prefer, without the caller having to care.
package image

import (
	"image"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"os"

	"github.com/chai2010/webp"
	"github.com/disintegration/imaging"
)

// func Fit {{{

// Given the image point (ip), we want it to fit within wanted point (wp).
// Return the resulting dimensions and percentage to scale by to achieve it.
//
// The returning float64 is what to scale the image to, or 0 if no scaling needed.
func Fit(ip, wp image.Point, enlarge bool) (image.Point, float64) {
	// Quick exit.
	//
	// If we do not need to enlarge, and both dimensions are less then wanted, nothing to do.
	if !enlarge && ip.X < wp.X && ip.Y < wp.Y {
		return ip, 0
	}

	dx := float64(wp.X) / float64(ip.X)
	dy := float64(wp.Y) / float64(ip.Y)
	by := dx

	if dy < dx {
		by = dy
	}

	np := image.Point{
		X: int(float64(ip.X) * by),
		Y: int(float64(ip.Y) * by),
	}

	return np, by
} // }}}

// func LoadReader {{{

// Given an io.Reader attempt to load an image from it.
//
// The image will be rotated automatically if needed.
func LoadReader(r io.Reader) (image.Image, error) {
	// As this uses image.Decode(), this will still work with any format registered with image, such as WebP above.
	// Though the AutoOrientation only works with JPEG, even though the other formats do support EXIF.
	return imaging.Decode(r, imaging.AutoOrientation(true))
} // }}}

// func SaveImageJPEG {{{

func SaveImageJPEG(w io.Writer, img image.Image) error {
	return imaging.Encode(w, img, imaging.JPEG, imaging.JPEGQuality(95))
} // }}}

// func SaveImagePNG {{{

func SaveImagePNG(w io.Writer, img image.Image) error {
	return imaging.Encode(w, img, imaging.PNG, imaging.PNGCompressionLevel(png.DefaultCompression))
} // }}}

// func SaveImageWebP {{{

func SaveImageWebP(w io.Writer, img image.Image) error {
	return webp.Encode(w, img, nil)
} // }}}

// func Open {{{

// Given a file name attempt to load an image from it.
//
// The image will be rotated automatically if needed.
func Open(file string) (image.Image, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	img, err := LoadReader(f)
	f.Close()

	return img, err
} // }}}

// func Resize {{{

// Resizes an image based on the interpolation options in the profile.
//
// I compared threee Go packages to handle this -
//
//   github.com/disintegration/imaging
//   github.com/nfnt/resize
//   github.com/rwcarlsen/goexif/exif
//
// On x86 and amd64 I got one result, but on ARMv5 is was a whole other story.
// imaging, which I prefered on x86 worked horribly on ARMv5.
// While the rotation for imaging worked a whole lot better, Resize took far, far longer.
//
// So I am sticking with nfnt for resizing, as it works best across all platforms I care about.
//
// Difference? 1s vs 10m for 1 image, and 2s vs. 22m for another.
func Resize(img image.Image, size image.Point) image.Image {
	return imaging.Resize(img, size.X, size.Y, imaging.Lanczos)
} // }}}

// func ImageToPrefer {{{

// Converts a provided image.Image to image.RGBA format.
func ImageToPrefer(in image.Image) *image.NRGBA {

	// First basic check - Is the image already a NRGBA?
	if nrgba, ok := in.(*image.NRGBA); ok {
		// Yep, no conversion needed then.
		return nrgba
	}

	// So we have to convert. Doing this all ourselves is a pain in the ass, so rather we let Go do it.
	// We create a new image.NRGBA of the same size and copy the pixels to it letting Go handle all the converisions.
	//
	// Get the size of the original image.
	bnds := in.Bounds()

	// Now make a new RGBA image with that size.
	nrgba := image.NewNRGBA(bnds)

	// Copy the source to the destination.
	draw.Draw(nrgba, bnds, in, image.ZP, draw.Src)

	return nrgba
} // }}}
