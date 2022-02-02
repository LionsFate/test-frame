package cmanager

import (
	"context"
	"frame/types"
	"image"

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

// func CManager.CacheImage {{{

func (cm *CManager) CacheImage(img image.Image) (uint64, error) {
	fl := cm.l.With().Str("func", "CacheImage").Logger()

	fl.Debug().Send()
	return 0, nil
} // }}}

// func CManager.LoadImage {{{

func (cm *CManager) LoadImage(id uint64) (image.Image, error) {
	fl := cm.l.With().Str("func", "LoadImage").Uint64("id", id).Logger()

	fl.Debug().Send()

	return nil, nil
} // }}}

