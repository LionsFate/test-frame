package cmanager

import (
	"bytes"
	"context"
	"hash"
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

	"github.com/rs/zerolog"
)

type hashReader struct {
	h hash.Hash
	r io.Reader
}

// func hashReader.Read {{{

// Basically an io.Reader that passes the read bytes for hashing before returning.
//
// Allows to read from an io.Reader while also hashing the contents at the same time.
func (h *hashReader) Read(p []byte) (n int, err error) {
	n, err = h.r.Read(p)

	// Hash the results first.
	h.h.Write(p)

	return n, err
} // }}}

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
func (cm *CManager) getID(hr *hashReader) (uint64, string, error) {
	fl := cm.l.With().Str("func", "getID").Logger()

	// Get the string hex value.
	tHash := hex.EncodeToString(hr.h.Sum(nil))

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

	// Our cache is stored as WebP.
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

	hr := &hashReader{
		h: sha256.New(),
		r: f,
	}

	co := cm.getConf()

	// Get a lock to throttle our resource usage if we need one.
	if co.BeNice {
		cm.beNice.Lock()
		defer cm.beNice.Unlock()
	}

	// Load the image from our buffer.
	img, err := fimg.LoadReader(hr)
	if err != nil {
		fl.Err(err).Msg("LoadReader")
		return 0, err
	}

	// Get the dimensions to resize if needed.
	size := img.Bounds().Size()

	// Lets see if we need to resize the image or not.
	newSize, _ := fimg.Fit(size, co.MaxResolution, false)

	// Is the size different?
	if newSize != size {
		start := time.Now()
		img = fimg.Resize(img, newSize)
		fl.Debug().Stringer("old", size).Stringer("new", newSize).Stringer("took", time.Since(start)).Msg("resize")
	}

	// Lets get the ID
	id, hash, err := cm.getID(hr)
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
	fo, err := os.Create(file + ".tmp")
	if err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("Create")
		return id, err
	}

	if err := fimg.SaveImageWebP(fo, img); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("Encode")
		fo.Close()
		return id, err
	}

	// We do not defer the close since we want to ensure we close the file
	// before we rename it.
	fo.Close()

	// File written without issue so rename it properly.
	if err := os.Rename(file+".tmp", file); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("Rename")
		return id, err
	}

	fl.Debug().Uint64("id", id).Str("hash", hash).Stringer("took", time.Since(s)).Msg("cached")
	return id, nil
} // }}}

// func CManager.LoadImage {{{

func (cm *CManager) LoadImage(id uint64, fit image.Point, enlarge bool) (image.Image, error) {
	var change float64

	fl := cm.l.With().Str("func", "LoadImage").Uint64("id", id).Logger()

	co := cm.getConf()

	// Get a lock to throttle our resource usage if we need one.
	if co.BeNice {
		cm.beNice.Lock()
		defer cm.beNice.Unlock()
	}

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

	img, err := fimg.LoadReader(f)
	if err != nil {
		fl.Err(err).Str("file", file).Msg("LoadReader")
		return nil, err
	}

	// Get the dimensions for resizing.
	size := img.Bounds().Size()

	newSize, change := fimg.Fit(size, fit, enlarge)

	if change != 0 {
		start := time.Now()

		img = fimg.Resize(img, newSize)

		fl.Debug().Stringer("old", size).Stringer("new", newSize).Stringer("wanted", fit).Float64("change", change).Stringer("took", time.Since(start)).Msg("resize")
	}

	return img, nil
} // }}}
