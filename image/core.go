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
	//"github.com/anthonynsimon/bild/transform"
	"github.com/disintegration/imaging"
	"github.com/rwcarlsen/goexif/exif"
	"image"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// var imgExtensions {{{

var imgExtensions = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".gif":  true,
	".png":  true,
} // }}}

// func Shrink {{{

// Checks if the given image size (orig) needs to be shrunk to fit our dimensions (toFit).
//
// It returns the original dimenstions if no change to the size is needed.
func Shrink(orig, toFit image.Point) image.Point {
	diffX := toFit.X - orig.X
	diffY := toFit.Y - orig.Y

	if diffX >= 0 && diffY >= 0 {
		// No changes needed, so just return our input.
		return orig
	}

	newX := orig.X
	newY := orig.Y

	// Width (X) larger?
	if diffX < 0 {
		shrunkBy := float32(toFit.X) / float32(newX)
		newX = toFit.X
		newY = int(float32(newY) * shrunkBy)
		diffY = toFit.Y - newY
	}

	// Is Height (Y) to large?
	// Note that the above could have shrunk Y, but it can still be too large.
	if diffY < 0 {
		shrunkBy := float32(toFit.Y) / float32(newY)
		newY = toFit.Y
		newX = int(float32(newX) * shrunkBy)
	}

	return image.Point{newX, newY}
} // }}}

// func Enlarge {{{

func Enlarge(orig, toFit image.Point) image.Point {
	diffX := toFit.X - orig.X
	diffY := toFit.Y - orig.Y

	if diffX < 0 && diffY < 0 {
		// No changes needed, so just return our input.
		return orig
	}

	newX := orig.X
	newY := orig.Y

	// When enlarging we only want to do so once, so we don't check both sides like with Shrink()
	if diffX < diffY {
		// Width is the closest, so lets increase by this.
		newY = int(float32(newY) * (float32(toFit.X) / float32(orig.X)))
		newX = toFit.X
	} else {
		// Height is the closest, so lets increase by this side.
		newX = int(float32(newX) * (float32(toFit.Y) / float32(orig.Y)))
		newY = toFit.Y
	}

	// Now in increasing the size on one side, we made have made the other side too large.
	// To fix this, we run it through Shrink() to be sure.
	return Shrink(image.Point{newX, newY}, toFit)
} // }}}

// func Fit {{{

// This handles returning an image size (orig) that fits (either enlargers or shrinks) to the requested (toFit) dimensions.
//
// This combines both Shrink() and Enlarge()
func Fit(orig, toFit image.Point) image.Point {
	diffX := toFit.X - orig.X
	diffY := toFit.Y - orig.Y

	if diffX < 0 || diffY < 0 {
		return Shrink(orig, toFit)
	}

	if diffX > 0 || diffY > 0 {
		return Enlarge(orig, toFit)
	}

	return orig
} // }}}

// func Open {{{

func Open(file string) (image.Image, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	// Attempt to read the image.
	img, _, err := image.Decode(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	f.Close()
	return img, nil
} // }}}

// func OpenRotate {{{

// The returns an image, handling any needed rotation.
//
// If the image was rotated then the bool is set to true, otherwise its false.
//
// If the file does not need to be rotated then nil is returned with no error.
func OpenRotate(file string) (*image.NRGBA, error) {
	// We know we need the image regardless, so lets go ahead and open it.
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	// Attempt to read the image.
	img, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}

	nrgba := ImageToPrefer(img)

	// Can this image contain EXIF data?
	if !CanExif(file) {
		return nrgba, nil
	}

	// Ok, it can contain exif, does it have any rotation data in it?
	f.Seek(0, io.SeekStart)
	rot := MustRotate(f)
	if rot == 0 {
		return nrgba, nil
	}

	/*
		rotOpts := &transform.RotationOptions{
			ResizeBounds: true,
			Pivot:        nil,
		}*/

	// The transform function is clockwise, while the orentiation is counter-clockwise.
	// Yeah, what fun.
	//
	// Ok, we neeed to rotate it.
	switch rot {
	case 1:
	case 2:
		//img = transform.FlipV(img)
		nrgba = imaging.FlipV(nrgba)
	case 3:
		//nrgba = transform.Rotate(nrgba, 180, rotOpts)
		nrgba = imaging.Rotate180(nrgba)
	case 4:
		//nrgba = transform.FlipV(nrgba)
		nrgba = imaging.FlipV(nrgba)
		//nrgba = transform.Rotate(nrgba, 180, rotOpts)
		nrgba = imaging.Rotate180(nrgba)
	case 5:
		//nrgba = transform.FlipV(nrgba)
		nrgba = imaging.FlipV(nrgba)
		//nrgba = transform.Rotate(nrgba, 90, rotOpts)
		nrgba = imaging.Rotate270(nrgba)
	case 6:
		//nrgba = transform.Rotate(nrgba, 90, rotOpts)
		nrgba = imaging.Rotate270(nrgba)
	case 7:
		//nrgba = transform.FlipV(nrgba)
		nrgba = imaging.FlipV(nrgba)
		//nrgba = transform.Rotate(nrgba, -90, rotOpts)
		nrgba = imaging.Rotate90(nrgba)
	case 8:
		//nrgba = transform.Rotate(nrgba, -90, rotOpts)
		nrgba = imaging.Rotate90(nrgba)
	}

	return nrgba, nil
} // }}}

// func MustRotate {{{

// This returns the rotation of the file if found in an exif header.
//
// This takes a file that it is assumed the caller has some expectation of it including EXIF data (mainly a JPG).
//
// If 0 is returned then no rotation was found.
func MustRotate(f *os.File) int {
	ex, err := exif.Decode(f)
	if err != nil {
		return 0
	}

	// Lets see if orientation is found in the exif ...
	o, err := ex.Get(exif.Orientation)
	if err != nil {
		return 0
	}

	// The tiff.Tag type returned likes to panic, which we don't generally like.
	// So we convert the value to a string, which is less prone to panic, and then
	// attempt to convert it to an integeer ourselves.
	i, err := strconv.Atoi(o.String())
	if err != nil {
		return 0
	}

	return i
} // }}}

// func IsImage {{{

// Returns true if the provided file can be an image, based only the file extension.
func IsImage(file string) bool {
	// If the name is too short it can't match.
	//
	// Shortest we can match is 5 bytes, something like "1.jpg".
	if len(file) < 5 {
		return false
	}

	// Get the extension.
	ext := strings.ToLower(filepath.Ext(file))

	if ext == "" {
		return false
	}

	if good, ok := imgExtensions[ext]; ok {
		return good
	}

	return false
} // }}}

// func CanExif {{{

// Returns true if the provided file typically can have exif data (such as a JPG).
func CanExif(file string) bool {
	// If the name is too short it can't match.
	//
	// Shortest we can match is 5 bytes, something like "1.jpg".
	if len(file) < 5 {
		return false
	}

	// Get the extension.
	ext := strings.ToLower(filepath.Ext(file))

	switch ext {
	case ".jpg":
		return true
	case ".jpeg":
		return true
	}

	return false
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
func Resize(img image.Image, size image.Point) *image.NRGBA {
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
