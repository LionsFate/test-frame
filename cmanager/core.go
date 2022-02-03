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
	"image/png"
	"os"
	"sync"

	"github.com/disintegration/imaging"
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

	file := path + "/" + hash + ".png"

	fl.Debug().Str("file", file).Send()

	return file, nil
} // }}}

// func CManager.CacheImage {{{

func (cm *CManager) CacheImage(img image.Image) (uint64, error) {
	fl := cm.l.With().Str("func", "CacheImage").Logger()

	co := cm.getConf()

	// Lets see if we need to resize the image or not.
	oldSize := img.Bounds()
	newSize := fimg.Shrink(oldSize.Max, co.MaxResolution)
	
	// Is the size different?
	if newSize != oldSize.Max {
		fl.Info().Stringer("old", oldSize.Max).Stringer("new", newSize).Msg("resize")
		img = fimg.Resize(img, newSize)
	}

	buf := cm.bp.Get().(*bytes.Buffer)
	buf.Reset()

	// Put our buffer back in the pool when done with it.
	defer cm.bp.Put(buf)

	// Write out the image to our buffer.
	if err := imaging.Encode(buf, img, imaging.PNG, imaging.PNGCompressionLevel(png.DefaultCompression)); err != nil {
		fl.Err(err).Msg("imaging.Encode")
		return 0, err
	}

	// Get the raw image slice for hashing and writing to a file.
	imgBytes := buf.Bytes()

	// Lets get the ID
	id, hash, err := cm.getID(imgBytes)
	if err != nil {
		fl.Err(err).Send()
		return 0, err
	}

	// Get the path the hash should be written to.
	file, err := cm.getFileName(hash)
	if err != nil {
		fl.Err(err).Send()
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
	if err := os.WriteFile(file + ".tmp", imgBytes, 0644); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("WriteFile")
		return id, err
	}

	// File written without issue so rename it properly.
	if err := os.Rename(file + ".tmp", file); err != nil {
		fl.Err(err).Uint64("id", id).Str("hash", hash).Msg("Rename")
		return id, err
	}

	fl.Info().Uint64("id", id).Msg("cached")
	return id, nil
} // }}}

// func CManager.LoadImage {{{

func (cm *CManager) LoadImage(id uint64) (image.Image, error) {
	fl := cm.l.With().Str("func", "LoadImage").Uint64("id", id).Logger()

	fl.Debug().Send()

	return nil, nil
} // }}}

