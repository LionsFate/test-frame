package cmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	fimg "frame/image"
	"frame/types"
	"image"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidbyttow/govips/v2/vips"
	"github.com/rs/zerolog"
)

// func New {{{

func New(confFile string, im types.IDManager, l *zerolog.Logger, ctx context.Context) (*CManager, error) {
	var err error

	cm := &CManager{
		l:     l.With().Str("mod", "cmanager").Logger(),
		im:    im,
		cFile: confFile,
		ctx:   ctx,
	}

	// Create our buffer pool so we can reuse the buffers for hasing
	// and rendering images.
	//
	// Almost the same as the sync.Pool documentation.
	cm.bp = sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}

	fl := cm.l.With().Str("func", "New").Logger()

	// Load our configuration.
	if err = cm.loadConf(); err != nil {
		return nil, err
	}

	// Start background configuration handling.
	cm.yc.Start()

	// We do not have any real background tasks, no database
	// connections, so no need for a background goroutine to handle
	// any shutdown here.

	fl.Debug().Send()

	return cm, nil
} // }}}

// func CManager.getID {{{

// Hashes the provided image and returns the ID as assigned by the IDManager.
func (cm *CManager) getID(imgBytes []byte) (uint64, string, error) {
	fl := cm.l.With().Str("func", "getID").Logger()

	// For hashing.
	h := sha256.New()

	// Hash the image.
	h.Write(imgBytes)

	// Get the string hex value.
	tHash := hex.EncodeToString(h.Sum(nil))

	tID, err := cm.im.GetID(tHash)
	if err != nil {
		fl.Err(err).Msg("GetID")
		return 0, "", err
	}

	return tID, tHash, nil
} // }}}

// func CManager.getConf {{{

func (cm *CManager) getConf() *conf {
	if co, ok := cm.co.Load().(*conf); ok {
		return co
	}

	fl := cm.l.With().Str("func", "getconf").Logger()

	// This should really never be able to happen.
	//
	// If this does, then there is a deeper issue.
	fl.Warn().Msg("Missing conf?")
	return &conf{}
} // }}}

// func CManager.getFileName {{{

// Returns the full path and name of the file on the file that
// should be written in the cache for the given hash.
func (cm *CManager) getFileName(hash string) (string, error) {
	fl := cm.l.With().Str("func", "getFileName").Str("hash", hash).Logger()

	co := cm.getConf()

	if len(hash) < 10 {
		return "", errors.New("invalid hash")
	}

	// Get the full path to the hash they want to write.
	path := co.ImageCache + "/" + string(hash[0]) + "/" + string(hash[1])

	// We only get called when someone wants to write a hash.
	//
	// Ensure the path exists so they can write.
	if _, err := os.Stat(path); err != nil {
		// We expect the path to not exist - Other errors though, we don't expect.
		if os.IsNotExist(err) {
			// Create the needed path(s)
			if err := os.MkdirAll(path, 0755); err != nil {
				fl.Err(err).Msg("mkdirall")
				return "", err
			}
			fl.Debug().Str("path", path).Msg("path created")
		} else {
			fl.Err(err).Str("path", path).Msg("exists check")
			return "", err
		}
	}

	file := path + "/" + hash + ".webp"

	fl.Debug().Str("file", file).Send()

	return file, nil
} // }}}

// func CManager.CacheImage {{{

func (cm *CManager) CacheImage(img image.Image) (uint64, error) {
	return 0, errors.New("not done")
} // }}}

// func CManager.CacheImageRaw {{{

func (cm *CManager) CacheImageRaw(f io.Reader) (uint64, error) {
	c := atomic.AddUint64(&cm.c, 1)
	s := time.Now()

	fl := cm.l.With().Str("func", "CacheImageRaw").Uint64("c", c).Logger()

	// Load the image from our buffer.
	img, err := cm.loadReader(f)
	if err != nil {
		fl.Err(err).Msg("loadReader")
		return 0, err
	}

	defer img.Close()

	// Rotate the image if needed.
	if img.HasExif() {
		if err := img.AutoRotate(); err != nil {
			fl.Err(err).Msg("AutoRotate")
			return 0, err
		}
	}

	co := cm.getConf()

	// Get the dimensions to resize if needed.
	size := image.Point{
		X: img.Height(),
		Y: img.Width(),
	}

	// Lets see if we need to resize the image or not.
	newSize := fimg.Shrink(size, co.MaxResolution)

	// Is the size different?
	if newSize != size {
		var shrink float64
		sizeX := float64(newSize.X) / float64(size.X)
		sizeY := float64(newSize.Y) / float64(size.Y)

		if sizeX > sizeY {
			shrink = sizeX
		} else {
			shrink = sizeY
		}

		start := time.Now()

		if err := img.Resize(shrink, vips.KernelAuto); err != nil {
			fl.Err(err).Msg("Resize")
			return 0, err
		}

		fl.Debug().Stringer("old", size).Stringer("new", newSize).Stringer("took", time.Since(start)).Msg("resize")

	}

	expar := vips.NewWebpExportParams()

	// For now we don't want to lose any quality of the original if possible.
	expar.NearLossless = true

	// Now lets get the bytes of the encoded image.
	nbuf, _, err := img.ExportWebp(expar)
	if err != nil {
		fl.Err(err).Msg("Export")
		return 0, err
	}

	// Lets get the ID
	id, hash, err := cm.getID(nbuf)
	if err != nil {
		fl.Err(err).Msg("getID")
		return 0, err
	}

	// Get the path the hash should be written to.
	file, err := cm.getFileName(hash)
	if err != nil {
		fl.Err(err).Msg("getFileName")
		return 0, err
	}

	if _, err := os.Stat(file); err == nil {
		// No error on stat, so the file exists.
		// Nothing more for us to do.
		fl.Debug().Uint64("id", id).Str("hash", hash).Msg("exists")
		return id, nil
	}

	// Write to a temporary file, so if we get an error we don't leave behind a partially written file
	// and potentially a broken image.
	if err := os.WriteFile(file+".tmp", nbuf, 0644); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("WriteFile")
		return id, err
	}

	// File written without issue so rename it properly.
	if err := os.Rename(file+".tmp", file); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("Rename")
		return id, err
	}

	fl.Debug().Uint64("id", id).Str("hash", hash).Stringer("took", time.Since(s)).Msg("cached")
	return id, nil
} // }}}

// func CManager.loadReader {{{

func (cm *CManager) loadReader(r io.Reader) (*vips.ImageRef, error) {
	fl := cm.l.With().Str("func", "loadReader").Logger()

	// Get a new buffer for this image.
	buf := cm.bp.Get().(*bytes.Buffer)
	buf.Reset()

	// Put our buffer back in the pool when done with it.
	defer cm.bp.Put(buf)

	// Read the image into our buffer.
	if _, err := buf.ReadFrom(r); err != nil {
		fl.Err(err).Msg("ReadFrom")
		return nil, err
	}

	ipar := vips.NewImportParams()

	// Load the image from our buffer.
	img, err := vips.LoadImageFromBuffer(buf.Bytes(), ipar)
	if err != nil {
		fl.Err(err).Msg("NewImageFromReader")
		return nil, err
	}

	return img, nil
} // }}}

// func CManager.LoadImage {{{

func (cm *CManager) LoadImage(id uint64, fit image.Point, enlarge bool) (image.Image, error) {
	fl := cm.l.With().Str("func", "LoadImage").Uint64("id", id).Logger()

	// Lets get the hash for this ID.
	hash, err := cm.im.GetHash(id)
	if err != nil {
		fl.Err(err).Msg("GetHash")
		return nil, err
	}

	// Have the hash, now need the file name in our cache.
	file, err := cm.getFileName(hash)
	if err != nil {
		fl.Err(err).Msg("getFileName")
		return nil, err
	}

	// Open the file for reading.
	f, err := os.Open(file)
	if err != nil {
		fl.Err(err).Str("file", file).Msg("Open")
		return nil, err
	}

	img, err := cm.loadReader(f)
	if err != nil {
		fl.Err(err).Str("file", file).Msg("loadReader")
		return nil, err
	}

	defer img.Close()

	// Get the dimensions for resizing.
	size := image.Point{
		X: img.Height(),
		Y: img.Width(),
	}

	// Do we shrink the image?
	newSize := fimg.Shrink(size, fit)

	// Is the size different?
	if newSize != size {
		var shrink float64
		sizeX := float64(newSize.X) / float64(size.X)
		sizeY := float64(newSize.Y) / float64(size.Y)

		if sizeX > sizeY {
			shrink = sizeX
		} else {
			shrink = sizeY
		}

		start := time.Now()

		if err := img.Resize(shrink, vips.KernelAuto); err != nil {
			fl.Err(err).Msg("Resize")
			return nil, err
		}

		fl.Debug().Stringer("old", size).Stringer("new", newSize).Stringer("took", time.Since(start)).Msg("resize")

		// Since we shrank the image, obviously not going to enlarge it now.
		enlarge = false
	}

	// Do we enlarge the image?
	if enlarge {
		newSize = fimg.Enlarge(size, fit)

		// Is the size different?
		if newSize != size {
			var enlar float64
			sizeX := float64(newSize.X) / float64(size.X)
			sizeY := float64(newSize.Y) / float64(size.Y)

			if sizeX > sizeY {
				enlar = sizeX
			} else {
				enlar = sizeY
			}

			start := time.Now()

			if err := img.Resize(enlar, vips.KernelAuto); err != nil {
				fl.Err(err).Msg("Resize")
				return nil, err
			}

			fl.Debug().Stringer("old", size).Stringer("new", newSize).Stringer("took", time.Since(start)).Msg("enlarge")
		}
	}

	exp := vips.NewDefaultPNGExportParams()

	out, err := img.ToImage(exp)
	if err != nil {
		fl.Err(err).Msg("ToImage")
		return nil, err
	}

	return out, nil
} // }}}
