package idmanager

import (
	"errors"
	"frame/yconf"
)

var ycCallers = yconf.Callers{
	Empty:   func() interface{} { return &conf{} },
	Merge:   yconfMerge,
	Changed: yconfChanged,
}

// func IDManager.loadConf {{{

func (im *IDManager) loadConf() error {
	var err error

	fl := im.l.With().Str("func", "loadConf").Logger()

	if im.yc, err = yconf.New(im.cFile, ycCallers, &im.l, im.ctx); err != nil {
		fl.Err(err).Msg("yconf.New")
		return err
	}

	if err = im.yc.CheckConf(); err != nil {
		fl.Err(err).Msg("yc.CheckConf")
		return err
	}

	fl.Debug().Interface("conf", im.yc.Get()).Send()

	// Get the loaded configuration
	co, ok := im.yc.Get().(*conf)
	if !ok {
		// This one should not really be possible, so this error needs to be sent.
		err := errors.New("invalid config loaded")
		fl.Err(err).Send()
		return err
	}

	if co == nil || co.Database == "" {
		err := errors.New("Missing database")
		fl.Err(err).Send()
		return err
	}

	if co.Queries.GetID == "" {
		err := errors.New("Missing getid query")
		fl.Err(err).Send()
		return err
	}

	if co.Queries.GetHash == "" {
		err := errors.New("Missing gethash query")
		fl.Err(err).Send()
		return err
	}

	// We need a new database connection before we can add the cache.
	db, err := im.dbConnect(co)
	if err != nil {
		fl.Err(err).Str("db", co.Database).Msg("new dbConnect")
		return err
	}

	im.db.Store(db)
	im.co.Store(co)

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

	if inA.Queries.GetID != inB.Queries.GetID && inB.Queries.GetID != "" {
		inA.Queries.GetID = inB.Queries.GetID
	}

	if inA.Queries.GetHash != inB.Queries.GetHash && inB.Queries.GetHash != "" {
		inA.Queries.GetHash = inB.Queries.GetHash
	}

	// First ensure A has the database if not empty.
	if inA.Database != inB.Database && inB.Database != "" {
		// Since inB is always the latest file opened, overwrite whatever is in inA.
		inA.Database = inB.Database
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

	if origConf.Database != newConf.Database {
		return true
	}

	if origConf.Queries.GetID != newConf.Queries.GetID {
		return true
	}

	if origConf.Queries.GetHash != newConf.Queries.GetHash {
		return true
	}

	return false
} // }}}
