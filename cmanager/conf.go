package cmanager

import (
	"errors"
	"fmt"
	"frame/yconf"
)

var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &confYAML{} },
	Merge:   yconfMerge,
	Convert: yconfConvert,
	Changed: yconfChanged,
}

// func CManager.loadConf {{{

func (cm *CManager) loadConf() error {
	var err error

	fl := cm.l.With().Str("func", "loadConf").Logger()

	if cm.yc, err = yconf.New(cm.cFile, ycCallers, &cm.l, cm.ctx); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	if err = cm.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	// Get the loaded configuration
	co, ok := cm.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	if co == nil {
		err := errors.New("Missing conf")
		fl.Err(err).Send()
		return err
	}

	fl.Debug().Interface("conf", co).Send()

	// Sane MaxResolution, no smaller then 720p, there is no upper bound.
	// If its lower then 720, then we default it to 4k.
	if co.MaxResolution.X < 720 {
		co.MaxResolution.X = 3840
	}

	if co.MaxResolution.Y < 720 {
		co.MaxResolution.Y = 3840
	}

	if co.ImageCache == "" {
		err := errors.New("Missing imagecache")
		fl.Err(err).Send()
		return err
	}

	cm.co.Store(co)

	return nil
} // }}}

// func yconfMerge {{{

func yconfMerge(inAInt, inBInt interface{}) (interface{}, error) {
	// Its important to note that previouisly loaded files are passed in a inA, where as inB is just the most recent.
	// This means that for our various maps, inA will continue to grow as the number of files we process grow, but inB will always be just the
	// most recent.
	//
	// So merge everything into inA.
	inA, ok := inAInt.(*conf)
	if !ok {
		return nil, errors.New("not a *conf")
	}

	inB, ok := inBInt.(*conf)
	if !ok {
		return nil, errors.New("not a *conf")
	}

	if inA.ImageCache != inB.ImageCache && inB.ImageCache != "" {
		inA.ImageCache = inB.ImageCache
	}

	// Copy MaxResolution if needed.
	if inA.MaxResolution != inB.MaxResolution {
		if inB.MaxResolution.X > 0 {
			inA.MaxResolution.X = inB.MaxResolution.X
		}

		if inB.MaxResolution.Y > 0 {
			inA.MaxResolution.Y = inB.MaxResolution.Y
		}
	}

	return inA, nil
} // }}}

// func yconfChanged {{{

func yconfChanged(origConfInt, newConfInt interface{}) bool {
	// None of these casts should be able to fail, but we like our sanity.
	origConf, ok := origConfInt.(*conf)
	if !ok {
		return true
	}

	newConf, ok := newConfInt.(*conf)
	if !ok {
		return true
	}

	if origConf.ImageCache != newConf.ImageCache {
		return true
	}

	if origConf.MaxResolution != newConf.MaxResolution {
		return true
	}

	return false
} // }}}

// func yconfConvert {{{

func yconfConvert(inInt interface{}) (interface{}, error) {
	in, ok := inInt.(*confYAML)
	if !ok {
		return nil, errors.New("not *confYAML")
	}

	out := &conf{
		ImageCache: in.ImageCache,
	}

	// Convert MaxResolution, if set.
	if in.MaxResolution != "" {
		num, err := fmt.Sscanf(in.MaxResolution, "%dx%d", &out.MaxResolution.X, &out.MaxResolution.Y)
		if err != nil || num != 2 {
			return nil, errors.New("invalid MaxResolution")
		}
	}

	return out, nil
} // }}}
