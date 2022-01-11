// Frame image processing.
//
// The package handles loading of all tags as well as caching of all images.
//
// The image cache is created and images are automatically rotated and resized as needed based on the "maxresolution" configuration option.
//
// Processes images found in configured paths, processes the files based on maximum configured sizes and loads the data into the database.
//
// Note - If you get bugs "no such file or directory" when reading from a network?
//
//  https://github.com/golang/go/issues/39237
//
// I got this during development, set GODEBUG=asyncpreemptoff=1
package imgproc

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	fimg "frame/image"
	"frame/tags"
	"frame/types"
	"image/png"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/disintegration/imaging"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog"
)

var emptyTime = time.Time{}

// func getFileType {{{

// Returns if the file is an image or sidecar.
//
// If its an image, 1 is returned and the 2nd value can be ignored.
//
// If its a sidecar 2 (txt) is returned, and the name of the base image (removing the .txt) is returned.
//
// This has potential to be expanded, such as with .xmp files.
// I had that at one point, but there were so many variances I decided to just simplify this
// code and read only a .txt file, with a single tag per line.
//
// Figure users can convert any other tag formats to a simple line-per text file easily enough.
//
// Returns 0 if the file is none of the above.
func getFileType(file string) (int, string) {
	// If the name is too short it can't match.
	//
	// Shortest we can match is 5 bytes, something like "1.jpg".
	if len(file) < 5 {
		return 0, ""
	}

	// Get the extension.
	ext := strings.ToLower(filepath.Ext(file))

	switch ext {
	case ".jpg":
		return 1, ""
	case ".jpeg":
		return 1, ""
	case ".gif":
		return 1, ""
	case ".png":
		return 1, ""
	case ".txt":
		// Its a sidecar - But is it for an image?
		// If its for example, 1.mp4.txt, we don't really care.
		nfile := file[:len(file)-4]
		if ft, _ := getFileType(nfile); ft == 1 {
			return 2, nfile
		}

		// Not a valid sidecar
		return 0, ""
	}

	return 0, ""
} // }}}

// func nextLoop {{{

// Just picks the next loop number to use, some random number range I picked.
// The number doesn't really matter, just want to avoid small numbers and wrap around to small numbers to keep some sanity.
func nextLoop(old uint32) uint32 {
	if old < 10 || old > 50000000 {
		return 10
	}

	return old + 1
} // }}}

// func New {{{

// Creates a new ImageProc.
//
// Checks the configuration, database and loads the cache but does not do any actual processing until Start() is called.
func New(confPath string, tm types.TagManager, l *zerolog.Logger, ctx context.Context) (*ImageProc, error) {
	ip := &ImageProc{
		l:     l.With().Str("mod", "imgproc").Logger(),
		tm:    tm,
		ctx:   ctx,
		cPath: confPath,
	}

	fl := ip.l.With().Str("func", "New").Logger()

	// Set an empty cache.
	ip.ca = &cache{
		bases: make(map[int]*baseCache, 1),
	}

	// Load our configuration.
	//
	// This also ensures that ip.getConf() can not fail.
	if err := ip.loadConf(); err != nil {
		return nil, err
	}

	// loadConf() above already connected to the database and setup our prepared statements, but it didn't load the cache.
	//
	// So handle that aspect here.
	if err := ip.loadCache(); err != nil {
		return nil, err
	}

	// All seems well, so lets start the real work before we return.
	//
	// Start background processing to watch configuration for changes.
	ip.yc.Start()

	// For the first run at startup always force a full check on all paths.
	//
	// This avoids problems where we were in the middle of a run and we got interupted.
	//
	// This can cause some paths to be in the database but not others, leaving to the possibility of orphaned paths
	// just not being checked if a full wasn't forced.
	for _, bc := range ip.ca.bases {
		bc.force = true
	}

	// Start the first check()
	ip.checkAll()

	// Background maintenance
	go ip.loopy()

	fl.Debug().Send()

	return ip, nil
} // }}}

// func ImageProc.dbConnect {{{

func (ip *ImageProc) dbConnect(co *conf) (*pgxpool.Pool, error) {
	var err error
	var db *pgxpool.Pool

	poolConf, err := pgxpool.ParseConfig(co.Database)
	if err != nil {
		return nil, err
	}

	// Set the log level properly.
	cc := poolConf.ConnConfig
	cc.LogLevel = pgx.LogLevelInfo
	cc.Logger = zerologadapter.NewLogger(ip.l)

	// So that each connection creates our prepared statements.
	poolConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := ip.setupDB(co, conn); err != nil {
			return err
		}

		return nil
	}

	if db, err = pgxpool.ConnectConfig(ip.ctx, poolConf); err != nil {
		return nil, err
	}

	return db, nil
} // }}}

// func ImageProc.loadTagFile {{{

// Loads the tags from the provided tag file.
//
// On success it returns the new tags and no error.
//
// On failure returns no tags and an error.
func (ip *ImageProc) loadTagFile(cr *checkRun, pc *pathCache, file, image string, modTime time.Time) error {
	name := pc.Path + "/" + file

	fl := ip.l.With().Str("func", "loadTagFile").Int("base", cr.bc.Base).Str("file", name).Logger()

	var newTags tags.Tags

	// Get the fileCache first, also avoids reading sidecars for files that don't exist.
	fc, err := ip.getFileCache(cr, pc, image, emptyTime)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		fl.Err(err).Send()
		return err
	}

	// Update our loop
	fc.loopS = pc.loop

	// Did the time on the sidecar change?
	ptime := modTime.Round(time.Millisecond)
	if ptime.Equal(fc.SideTS) {
		// Time is the same, so nothing more to do.
		return nil
	}

	fl.Info().Msg("Time changed")
	fc.SideTS = ptime
	pc.updated |= upPathFI
	fc.updated |= upSideTS

	// Now open the sidecar for reading.
	f, err := cr.bc.bfs.Open(name)
	if err != nil {
		fl.Err(err).Send()
		return fmt.Errorf("open(%s): %s", name, err)
	}

	defer f.Close()

	// Our new buffer for reading a single line at a time.
	buf := bufio.NewReader(f)

	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			fl.Err(err).Send()
			return fmt.Errorf("read(%s): %w", name, err)
		}

		// Strip any spaces from tag.
		line = strings.TrimSpace(line)

		// Skip empty tags, as well as absurdly long tags (WTH dude?)
		if line == "" || len(line) > 100 {
			continue
		}

		// Get the tag from TagManager.
		tag, err := ip.tm.Get(line)
		if err != nil {
			fl.Err(err).Send()
			return err
		}

		// Zero tag? For some reason the TagManager doesn't care for this tag, so skip it.
		if tag == 0 {
			continue
		}

		// Add the tag
		newTags = newTags.Add(tag)
	}

	// Fix the tags
	newTags = newTags.Fix()

	// Did the tags change?
	if !fc.SideTG.Equal(newTags) {
		fc.SideTG = newTags
		pc.updated |= upPathFI
		fc.updated |= upSideTG
	}

	return nil
} // }}}

// func ImageProc.getFileCache {{{

func (ip *ImageProc) getFileCache(cr *checkRun, pc *pathCache, file string, modTime time.Time) (*fileCache, error) {
	name := pc.Path + "/" + file

	fl := ip.l.With().Str("func", "getFileCache").Int("base", cr.bc.Base).Str("file", name).Logger()

	// Get the file cache.
	fc, ok := pc.Files[file]
	if !ok {
		// Doesn't exist yet, so go ahead and create it.
		//
		// Note we don't update any bit flags or set the paths UpPathFI here, as
		// if emptyTime is passed in we are being called from a sidecar.
		//
		// We only update those when we have a time of the file itself and not a sidecar.
		fl.Debug().Msg("Created")
		fc = &fileCache{
			Name: file,
		}
		pc.Files[file] = fc
	}

	// If the file was seen this loop already then skip the below checks.
	if fc.loopF == pc.loop {
		return fc, nil
	}

	// Time provided?
	if modTime == emptyTime {
		// Nope, so just skip the rest of the checks below.
		//
		// We do not update the loop though, as since we don't have the modTime, we haven't actually seen
		// the file itself (could be a sidecar we were called from).
		return fc, nil
	}

	// Update the loop this was seen on
	fc.loopF = pc.loop

	// Update the last modified time?
	ptime := modTime.Round(time.Millisecond)
	if ptime.Equal(fc.FileTS) {
		return fc, nil
	}

	fl.Info().Msg("Time changed")
	fc.FileTS = ptime
	fc.updated |= upFileTS
	pc.updated |= upPathFI

	// The timestamp of the file changed.
	//
	// If it was an error, this can mean someone fixed the problem, so go ahead and clear the error.
	if fc.fileError {
		fc.fileError = false
	}

	return fc, nil
} // }}}

// func ImageProc.getPathCache {{{

func (ip *ImageProc) getPathCache(cr *checkRun, path string, inheritTags tags.Tags, inherit bool) (*pathCache, error) {
	fl := ip.l.With().Str("func", "getPathCache").Int("base", cr.bc.Base).Str("path", path).Logger()

	// Get the path cache.
	pc, ok := cr.bc.Paths[path]
	if !ok {
		fl.Debug().Msg("Created")
		pc = &pathCache{
			Path:    path,
			Tags:    inheritTags,
			Files:   make(map[string]*fileCache, 1),
			updated: upPathTG,
		}
		cr.bc.Paths[path] = pc
	}

	// Update the loop
	pc.loop = cr.bc.loop

	// We need the timestamp for our path first.
	file, err := cr.bc.bfs.Open(path)
	if err != nil {
		fl.Err(err).Msg("open")
		return nil, fmt.Errorf("open(%s): %w", path, err)
	}

	fstat, err := file.Stat()
	if err != nil {
		file.Close()
		fl.Err(err).Msg("stat")
		return nil, fmt.Errorf("stat(%s): %w", path, err)
	}

	file.Close()

	// Update the last modified time?
	//
	// Note that we round the ModTime() here to the millisecond, as I found that PostgreSQL does its own rounding of the number.
	// This would cause the value we INSERT to be different in the SELECT, and thus cause the times to never match properly.
	ptime := fstat.ModTime().Round(time.Millisecond)
	if !ptime.Equal(pc.Changed) {
		fl.Info().Msg("Time changed")
		pc.Changed = ptime
		pc.updated |= upPathTS
	}

	// Before we check the tags, does this path have specifically configured tags?
	cp, ok := cr.cb.Paths[path]
	if ok {
		if len(cp.Tags) > 0 {
			// Force inherit so we set the tags properly.
			inherit = true
			inheritTags = cp.Tags
		}
	}

	// Do we check the inherited tags?
	if inherit {
		// Did the tags change?
		if !inheritTags.Equal(pc.Tags) {
			fl.Info().Msg("Tags changed")
			pc.updated |= upPathTG
			pc.Tags = inheritTags
		}
	}

	fl.Debug().Send()

	return pc, nil
} // }}}

// func ImageProc.checkPathPartial {{{

func (ip *ImageProc) checkPathPartial(cr *checkRun, path string) error {
	fl := ip.l.With().Str("func", "checkPathPartial").Int("base", cr.bc.Base).Str("path", path).Logger()

	// We were given a path to check if it was modifed in any way to decide if we call checkBasePath()
	// or not on it.
	//
	// Now lets see if the path has been modified or not.
	pc, err := ip.getPathCache(cr, path, tags.Tags{}, false)
	if err != nil {
		fl.Err(err).Msg("getPathCache")
		return err
	}

	// Did anything in the path change?
	if pc.updated&(upPathTG|upPathTS) == 0 {
		// path has not changed.
		//
		// We assume all the files in this path in cache are still there and exactly the same.
		//
		// Any change to a file would also touch the directory modified time.
		//
		// Loop through all the files and update the loop so they don't disappear.
		//
		// Any new paths seen will be handled by checkBasePath() being recursive for new paths.
		//
		// NOTE: While this method is valid for all file systems I have tested, being that fs.FS is new it is possible some new type of
		// fs.FS might not properly set the paths modified time when a file has been modified. If this actually happens then we need some
		// way to force a full every loop for that FS type.
		//
		// However, be that has not happened yet, this is just a note how to handle something that hopefuly never happens in general.
		for _, file := range pc.Files {
			file.loopF = pc.loop

			// Does this file also have a sidecar?
			if !file.SideTS.Equal(emptyTime) {
				// Yep, so go ahead and set that loop as well.
				file.loopS = pc.loop
			}
		}

		fl.Debug().Msg("unchanged")
		return nil
	}

	// The path changed, so hand off to checkBasePath()
	return ip.checkBasePath(cr, pc, path, false)
} // }}}

// func ImageProc.checkBasePath {{{

func (ip *ImageProc) checkBasePath(cr *checkRun, pc *pathCache, path string, full bool) error {
	fl := ip.l.With().Str("func", "checkBasePath").Int("base", cr.bc.Base).Str("path", path).Logger()
	fl.Debug().Send()

	// Lets get all the files within this path.
	files, err := fs.ReadDir(cr.bc.bfs, path)
	if err != nil {
		fl.Err(err).Msg("ReadDir")
		return err
	}

	for _, file := range files {
		// Directory?
		if file.IsDir() {
			// Get the new path name
			npath := path + "/" + file.Name()

			if path == "." {
				npath = file.Name()
			}

			// Is this a partial?
			if !full {
				// Is the path in the cache?
				if _, ok := cr.bc.Paths[npath]; ok {
					// Its a known path, which means our caller will handle it directly.
					continue
				}
			}

			// Either a full, or not in the cache.
			npc, err := ip.getPathCache(cr, npath, pc.Tags, true)
			if err != nil {
				return err
			}

			if err := ip.checkBasePath(cr, npc, npath, full); err != nil {
				return err
			}

			continue
		}

		nfl := fl.With().Str("file", file.Name()).Logger()

		// Is this a file we care about?
		ft, iname := getFileType(file.Name())
		switch ft {
		case 0:
			continue
		case 1:
			// Load the file info to pass to getFileCache, so it doesn't have to do a Stat() call.
			info, err := file.Info()
			if err != nil {
				nfl.Err(err).Msg("file.Info")
				return err
			}

			// Everything we need to do is handled by requesting the file cache.
			//
			// Hashing and sizing happens in the next phase of check()
			if _, err := ip.getFileCache(cr, pc, file.Name(), info.ModTime()); err != nil {
				nfl.Err(err).Send()
				return err
			}
		case 2:
			// Load the file info to pass to loadTagFile, so it doesn't have to do a Stat() call.
			info, err := file.Info()
			if err != nil {
				nfl.Err(err).Msg("file.Info")
				return err
			}

			// Load the tags
			if err := ip.loadTagFile(cr, pc, file.Name(), iname, info.ModTime()); err != nil {
				nfl.Err(err).Send()
				return err
			}
		default:
			nfl.Warn().Str("image", iname).Int("type", ft).Msg("Unsupported filetype")
			continue
		}
	}

	return nil
} // }}}

// func ImageProc.checkHashTagsDB {{{

// This calculates the file hash, creates the file in the hash path, and calculates the tags.
func (ip *ImageProc) checkHashTagsDB(cr *checkRun) error {
	var pathTags bool

	fl := ip.l.With().Str("func", "checkHashTags").Int("base", cr.bc.Base).Logger()

	loop := cr.bc.loop

	// Run through the paths in the base
	for _, pc := range cr.bc.Paths {
		// First, if the path itself wasn't seen, no need to check the files - They were all basically removed.
		//
		// We don't delete the path here, that happens in cleanCache().
		if pc.loop != loop {
			// Ensure the database removes the path (and files) properly.
			if err := ip.updateDBPF(cr, pc); err != nil {
				fl.Err(err).Msg("updateDBPF")
				return err
			}

			fl.Info().Str("path", pc.Path).Msg("path removed - skipped")
			continue
		}

		if pc.updated&upPathTG != 0 {
			pathTags = true
		} else {
			pathTags = false
		}

		// Run through the files
		for _, fc := range pc.Files {
			// If this file wasn't seen this loop, then skip it - Needs to be removed.
			if fc.loopF != loop {
				fl.Debug().Str("file", fc.Name).Msg("removed - skipped")
				continue
			}

			// Any tags change?
			//
			// Or, does the file itself not have any tags at all?
			if pathTags || fc.updated&upSideTG != 0 || len(fc.CTags) == 0 {
				// Lets calculate the new tags.
				nTags := tags.Tags{}
				nTags = nTags.Combine(pc.Tags)
				nTags = nTags.Combine(fc.SideTG)

				// Now did they actually change?
				if !nTags.Equal(fc.CTags) {
					fl.Info().Str("file", fc.Name).Msg("Tags changed")
					fc.CTags = nTags

					// Set that the calculated tags updated
					fc.updated |= upFileCT
					pc.updated |= upPathFI
				}
			}

			// If a file has no tags, we consider this to be an error.
			// All files must have at least 1 tag to be useful at all to us.
			//
			// You can add default tags just be adding path tags or tags to the
			// base itself, so this really just means a misconfiguration typically.
			//
			// We do not bother doing any update or further check on the file
			// when its missing its tags.
			if len(fc.CTags) == 0 {
				fl.Warn().Str("file", fc.Name).Msg("Has no tags")
				continue
			}

			// Did the file timestamp change?
			// Or, is there no hash already?
			if fc.updated&upFileTS != 0 || fc.Hash == "" {
				if err := ip.setFileHash(cr, pc, fc); err != nil {

					// We want to ensure one bad file can't crash the entire application, so we log the error here but otherwise we continue.
					// The file itself as flagged as being in an error state.
					//
					// Should the timestamp on the file change the error state will be cleared.
					fc.fileError = true
					fl.Err(err).Msg("getFileHash")
				}
			}
		}

		// Now update the database.
		if err := ip.updateDBPF(cr, pc); err != nil {
			fl.Err(err).Msg("updateDBPF")
			return err
		}
	}

	return nil
} // }}}

// func ImageProc.setFileHash {{{

// This updates the file hash and creates the physical resized file if it doesn't already exist
func (ip *ImageProc) setFileHash(cr *checkRun, pc *pathCache, fc *fileCache) error {
	name := pc.Path + "/" + fc.Name

	fl := ip.l.With().Str("func", "setFileHash").Int("base", cr.bc.Base).Str("path", pc.Path).Str("file", fc.Name).Logger()

	// Lets open the file for reading.
	f, err := cr.bc.bfs.Open(name)
	if err != nil {
		fl.Err(err).Msg("open")
		return err
	}

	// Now create the file hash
	hash := ip.getHash(cr.hash)

	// Lets read in the file to the hash
	if _, err := io.Copy(hash, f); err != nil {
		f.Close()
		fl.Err(err).Msg("copy-hash")
		return err
	}

	// Get the hash
	tHash := hex.EncodeToString(hash.Sum(nil))
	if tHash == fc.Hash {
		f.Close()
		// As the hash has not changed, there is no need to create a new image cache.
		return nil
	}

	// Update the file hash and set the bit so the database is properly changed.
	fc.Hash = tHash
	fc.updated |= upFileHS
	pc.updated |= upPathFI

	// Hash should never be this small, but we like sanity.
	if len(tHash) < 3 {
		f.Close()
		fl.Warn().Msg("bad hash")
		return errors.New("Bad hash??")
	}

	//fl.Info().Str("hash", tHash).Send()

	// Now, does the cache file already exist?
	//
	// This can happen for any number of reasons, as myself - I have many duplicate files, sadly, and not always the same tags for each.
	// So its possible another base, or even another file within the same base is a duplicate that already created the cache file.
	hPath := cr.cachePath + "/" + string(tHash[0]) + "/" + string(tHash[1])
	fName := hPath + "/" + tHash + ".png"

	if _, err := os.Stat(fName); err == nil {
		// No error on stat, so the file exists.
		// Nothing more for us to do.
		f.Close()
		fl.Info().Str("hash", tHash).Msg("exists")
		return nil
	}

	// Now we need to read the file in for loading the image.
	//
	// If the file can seek then we'll just seek, otherwise we have to re-open the file.
	if seek, ok := f.(io.Seeker); ok {
		if _, err = seek.Seek(0, io.SeekStart); err != nil {
			f.Close()
			fl.Err(err).Msg("seek")
			return err
		}
	} else {
		// Close the old file for reopening
		f.Close()

		f, err = cr.bc.bfs.Open(name)
		if err != nil {
			fl.Err(err).Msg("open")
			return err
		}
	}

	// Now we defer the close, we didn't do this above since we know we had to possibly close it (if its not an io.Seeker) and re-open.
	defer f.Close()

	// Check if the path exists or not
	if _, err := os.Stat(hPath); err != nil {
		// We expect the path to not exist - Other errors though, we don't expect.
		if os.IsNotExist(err) {
			// Create the needed path(s)
			if err := os.MkdirAll(hPath, 0755); err != nil {
				fl.Err(err).Msg("mkdirall")
				return err
			}
			fl.Info().Str("hpath", hPath).Msg("path created")
		} else {
			fl.Err(err).Str("hpath", hPath).Msg("exists check")
			return err
		}
	}

	// Ok, load the image so we can resize and cache it now.
	img, err := imaging.Decode(f, imaging.AutoOrientation(true))
	if err != nil {
		// Looks like the file isn't able to be decoded.
		fl.Err(err).Str("file", name).Msg("imaging.Decode")
		return err
	}

	// Now we possibly need to resize the image to fit our possible max bounds.
	oldSize := img.Bounds()
	newSize := fimg.Shrink(oldSize.Max, cr.maxResolution)

	// Do we need to resize it?
	if newSize != oldSize.Max {
		fl.Info().Str("name", name).Stringer("old", oldSize.Max).Stringer("new", newSize).Msg("resize")
		img = fimg.Resize(img, newSize)
	}

	// Now we write the image out to the cache location.
	newF, err := os.Create(fName)
	if err != nil {
		fl.Err(err).Str("file", fName).Msg("create")
		return err
	}

	// Write out the image.
	if err := imaging.Encode(newF, img, imaging.PNG, imaging.PNGCompressionLevel(png.DefaultCompression)); err != nil {
		fl.Err(err).Str("file", fName).Msg("imaging.Encode")
		newF.Close()
		return err
	}

	fl.Info().Msg("cached")

	// File created, done.
	newF.Close()

	return nil
} // }}}

// func ImageProc.checkBase {{{

// TODO Need to check if the database has the base setup, otherwise it just errors.
func (ip *ImageProc) checkBase(bc *baseCache) {
	fl := ip.l.With().Str("func", "checkBase").Int("base", bc.Base).Logger()
	start := time.Now()

	// We do not allow multiple instances of ourself to run.
	//
	// Main reason - Directory scanning time.
	//
	// Without this you can easily get into a situation where the scans just never end. One scan starts
	// on a directory with 100k files, a 2nd starts and slows down the first, eventually a 3rd slows
	// down the others, and its a never-ending cycle.
	//
	// Think of older rpi's and slower SD cards and one can easily see how this can be an issue even with a small number
	// of files.
	if !atomic.CompareAndSwapUint32(&bc.checkRun, 0, 1) {
		fl.Info().Msg("check already running")
		return
	}

	// Ensure we release the "lock" when finished.
	defer atomic.StoreUint32(&bc.checkRun, 0)

	bc.bMut.Lock()
	defer bc.bMut.Unlock()

	// Increase our loop
	bc.loop = nextLoop(bc.loop)

	// We need the base configuration as well.
	co := ip.getConf()

	cr := &checkRun{
		cachePath:     co.ImageCache,
		hash:          co.Hash,
		maxResolution: co.MaxResolution,
		cb:            co.Bases[bc.Base],
		bc:            bc,
	}

	// Simple check - No '.' path in the cache forces a full.
	if _, ok := bc.Paths["."]; !ok {
		bc.force = true
	}

	// Is this a forced full loop?
	if bc.force {
		// A full loop means check every path, every file (at least a stat for the modified time) for changes.
		pc, err := ip.getPathCache(cr, ".", bc.Tags, true)
		if err != nil {
			fl.Err(err).Msg("getPathCache")
			return
		}

		if err := ip.checkBasePath(cr, pc, ".", true); err != nil {
			fl.Err(err).Msg("checkBasePath")
			return
		}

		bc.force = false
	} else {
		// Not force, so lets do a partial scan.
		//
		// A partial scan is one where instead of looping every single path and checking every file, we assume
		// the cache we have is valid and only check the path modified time.
		//
		// For more details, see checkPathPartial()
		//
		// To do this we loop through the cache paths instead of the paths themselves, and checkBasePath() (called by
		// checkPathPartial()) becomes non-recursive (unless its a new path, then it will be recursive).
		//
		// Now this means that while we check all the paths the bc.Paths map can change.
		//
		// To avoid issues we copy the path names and loop through those names.
		paths := make([]string, 0, len(bc.Paths))
		for path, _ := range bc.Paths {
			paths = append(paths, path)
		}

		// So its easier in the logs to follow whats going on we sort the paths.
		sort.Strings(paths)

		for _, path := range paths {
			if err := ip.checkPathPartial(cr, path); err != nil {
				fl.Err(err).Msg("checkPathPartial")
				return
			}
		}
	}

	// Ok, now we calculate both the tags and hashes, create the physical cache file,
	// and update the database.
	if err := ip.checkHashTagsDB(cr); err != nil {
		fl.Err(err).Msg("checkHashTags")
		return
	}

	// Remove any cache entries that should no longer be there.
	//
	// We do this after the database so it can delete/disable any entries first before we clean them here.
	if err := ip.cleanCache(cr); err != nil {
		fl.Err(err).Msg("cleanCache")
		return
	}

	end := time.Since(start)
	fl.Info().Str("took", end.String()).Send()

	return
} // }}}

// func ImageProc.cleanCache {{{

// Cleans up the cache, removing any path or files that no longer exist (and are disabled in the database).
func (ip *ImageProc) cleanCache(cr *checkRun) error {
	fl := ip.l.With().Str("func", "cleanCache").Int("base", cr.bc.Base).Logger()

	loop := cr.bc.loop

	for path, pc := range cr.bc.Paths {
		for file, fc := range pc.Files {
			// Was the file seen this loop?
			if fc.loopF == loop {
				continue
			}

			// Does it exist in the database?
			if fc.id != 0 && !fc.disabled {
				continue
			}

			// Should be removed.
			fl.Info().Str("path", path).Str("file", file).Msg("cleaned")
			delete(pc.Files, file)
		}

		// Was this path seen this loop?
		if pc.loop == loop {
			// It was seen, so not possible to remove it.
			continue
		}

		// Does it still have valid files within?
		if len(pc.Files) > 0 {
			// How does this happen?
			//
			// When a path containing at least one file that was in the database was removed.
			//
			// The file has to be removed from the database first, and then on the next loop we should clean
			// both the file(s) and the path.
			continue
		}

		// Ok, no files - Is this path in the database and still enabled?
		if pc.id != 0 && !pc.disabled {
			// Yep, still has to be removed from the database
			continue
		}

		// No reason to keep the path - So remove it.
		fl.Info().Str("path", path).Msg("cleaned")
		delete(cr.bc.Paths, path)
	}

	return nil
} // }}}

// func ImageProc.updateDBPF {{{

// Handles updating the path and all files within said path to the database.
func (ip *ImageProc) updateDBPF(cr *checkRun, pc *pathCache) error {
	fl := ip.l.With().Str("func", "updateDBPF").Int("base", cr.bc.Base).Str("path", pc.Path).Logger()

	// Any changes to the path or the files within would update at least 1 bit in pc.updated.
	//
	// Even just a file change with no path change would set upPathFI.
	if pc.updated == 0 {
		return nil
	}

	// Need the database.
	db, err := ip.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// Get our transaction
	tx, err := db.Begin(ip.ctx)
	if err != nil {
		fl.Err(err).Msg("begin")
		return err
	}

	// Handle database path work.
	if err := ip.updateDBPath(tx, cr, pc); err != nil {
		fl.Err(err).Msg("updateDBPath")
		tx.Rollback(ip.ctx)
		return err
	}

	// Run through the files
	for _, fc := range pc.Files {
		if err := ip.updateDBFile(tx, cr, pc.id, fc); err != nil {
			fl.Err(err).Msg("updateDBFile")
			tx.Rollback(ip.ctx)
			return err
		}
	}

	if err = tx.Commit(ip.ctx); err != nil {
		fl.Err(err).Msg("commit")
		return err
	}

	// Now that we have committed the work, we can clear the changed flags.
	//
	// We do not do this before the commit in case of database error, so we can try the transaction again
	// later on.
	pc.updated = 0

	for _, fc := range pc.Files {
		if fc.updated != 0 {
			fc.updated = 0
		}
	}

	return nil
} // }}}

// func ImageProc.updateDBFile {{{

func (ip *ImageProc) updateDBFile(tx pgx.Tx, cr *checkRun, pid uint64, fc *fileCache) error {
	fl := ip.l.With().Str("func", "updateDBFile").Uint64("pid", pid).Str("file", fc.Name).Logger()

	// A file without any tags is of no value to the system, and can not be
	// inserted into the database.
	if len(fc.CTags) == 0 {
		fl.Warn().Msg("Has no tags")
		return nil
	}

	// Loop check - If we didn't see the file this loop we disable it.
	//
	// Note we don't check fileError yet, as this allows a previously errored file to be removed, and properly cleaned out here.
	if fc.loopF != cr.bc.loop {
		// File has no loop, does it have an ID?
		if fc.id == 0 {
			// No loop, and no ID?
			//
			// This happens if a sidecar exists without the file its meant to be the sidecar for.
			//
			// So does the sidecar loopS exist?
			if fc.loopS == cr.bc.loop {
				// Disable the file so its cleaned up and hopefully someone fixes it.
				fc.disabled = true
				return nil
			}

			// Ok, so the sidecar wasn't the cause, so this is a warning as this shouldn't happen.
			err := errors.New("no loop, no id")
			fl.Warn().Err(err).Send()
			fc.disabled = true
			return err
		}

		if fc.disabled {
			// Is the file disabled?
			//
			// Should have been cleaned up already in the clearCache(), so looks like another warning.
			err := errors.New("already disabled unseen file")
			fl.Warn().Err(err).Send()
			return err
		}

		// Lets update the database to disable the path
		if _, err := tx.Exec(ip.ctx, "files-disable", fc.id); err != nil {
			fl.Err(err).Uint64("fid", fc.id).Msg("disable file")
			return err
		}

		fc.disabled = true

		return nil
	}

	if fc.fileError {
		return nil
	}

	// Is this a new file?
	if fc.id == 0 {
		if err := tx.QueryRow(ip.ctx, "files-insert", pid, fc.Name, fc.FileTS, fc.Hash, fc.SideTS, fc.SideTG, fc.CTags).Scan(&fc.id); err != nil {
			fl.Err(err).Str("file", fc.Name).Msg("insert file")
			return err
		}

		fl.Debug().Str("file", fc.Name).Uint64("id", fc.id).Send()
	} else {
		// Existing path - So anything to update?
		if fc.updated&(upFileTS|upFileCT|upFileHS|upSideTS|upSideTG) != 0 {
			// Update the row
			if _, err := tx.Exec(ip.ctx, "files-update", fc.id, fc.FileTS, fc.Hash, fc.SideTS, fc.SideTG, fc.CTags); err != nil {
				fl.Err(err).Uint64("fid", fc.id).Msg("update file")
				return err
			}

			fl.Info().Msg("updated")
		}
	}

	// All looks good.
	return nil
} // }}}

// func ImageProc.updateDBPath {{{

func (ip *ImageProc) updateDBPath(tx pgx.Tx, cr *checkRun, pc *pathCache) error {
	fl := ip.l.With().Str("func", "updateDBPath").Int("base", cr.bc.Base).Str("path", pc.Path).Logger()

	// Loop check - If we didn't see the path this loop then we disable it.
	if pc.loop != cr.bc.loop {
		// If the path has no id then we don't have to update the database.
		//
		// Now exactly how a path shows up without an ID and not be seen? Thats a good question, it shouldn't.
		//
		// For a path to show up at all, the first time it has no ID but for that first loop we assign it an ID here.
		// After that the path can be removed and it would not be seen in the current loop, and then we disable it, but it should still
		// have an ID.
		//
		// So we log that it happened and let the caller handle this issue.
		if pc.id == 0 {
			err := errors.New("no loop, no id")
			fl.Err(err).Send()
			pc.disabled = true
			return err
		}

		if pc.disabled {
			// Path is already disabled?
			//
			// If it was disabled, the caller should have already removed it from memory the last time we disabled it, as
			// we should be the only function to disable a path, and we leave it up to our caller to remove the pathCache from memory.
			//
			// If this happens, that means the caller didn't remove it.
			//
			// That, or the other option is when we loaded the cache from the database we loaded a disabled path when we should skip those.
			//
			// Check the SQL if its including disabled paths when loading or not?
			err := errors.New("already disabled unseen path")
			fl.Err(err).Send()
			return err
		}

		// Lets update the database to disable the path
		if _, err := tx.Exec(ip.ctx, "paths-disable", pc.id); err != nil {
			fl.Err(err).Uint64("pid", pc.id).Msg("disable path")
			return err
		}

		pc.disabled = true

		return nil
	}

	// Is this a new path?
	if pc.id == 0 {
		if err := tx.QueryRow(ip.ctx, "paths-insert", cr.bc.Base, pc.Path, pc.Changed, pc.Tags).Scan(&pc.id); err != nil {
			fl.Err(err).Str("path", pc.Path).Msg("insert path")
			return err
		}

		fl.Debug().Str("path", pc.Path).Uint64("id", pc.id).Send()
	} else {
		// Existing path - So anything to update?
		if pc.updated&(upPathTG|upPathTS) != 0 {
			// Update the row
			if _, err := tx.Exec(ip.ctx, "paths-update", pc.id, pc.Changed, pc.Tags); err != nil {
				fl.Err(err).Uint64("pid", pc.id).Msg("update path")
				return err
			}
		}
	}

	// All looks good.
	return nil
} // }}}

// func ImageProc.loadCache {{{

// Loads the cache from the database.
//
// As this is meant to be called from the start of the program, it will replace any existing cache without care.
func (ip *ImageProc) loadCache() error {
	var inID uint64
	var name, hash string
	var changed, sidets time.Time
	var tgs, sideTags tags.Tags

	fl := ip.l.With().Str("func", "loadCache").Logger()

	// Lets load all the paths from the database first.
	db, err := ip.getDB()
	if err != nil {
		fl.Err(err).Msg("getDB")
		return err
	}

	// Lets loop through our configured bases and create our cache.
	co := ip.getConf()
	ca := ip.ca

	// We are pretty much replacing the entire cache here, so just get a lock over it until we are done.
	ca.cMut.Lock()
	defer ca.cMut.Unlock()

	for bid, cb := range co.Bases {
		// Create our base cache
		bc := &baseCache{
			Base:  bid,
			Tags:  cb.Tags,
			path:  cb.Path,
			Paths: make(map[string]*pathCache, 1),
		}

		// Since we are replacing the baseCache, we need to ensure its fs.FS is set correctly as well.
		bc.bfs = os.DirFS(cb.Path)

		// Set our base cache
		ca.bases[bid] = bc

		// And now run through the paths
		pathRows, err := db.Query(ip.ctx, "paths-select", bid)
		if err != nil {
			fl.Err(err).Msg("paths-select")
			return err
		}

		// Loop through our paths
		for pathRows.Next() {
			// Get the values
			if err := pathRows.Scan(&inID, &name, &changed, &tgs); err != nil {
				pathRows.Close()
				fl.Err(err).Msg("paths-select-rows-scan")
				return err
			}

			// Fix the tags first
			tgs = tgs.Fix()

			// Now add the path to our cache.
			pc := &pathCache{
				id:      inID,
				Path:    name,
				Tags:    tgs.Copy(),
				Changed: changed,
				Files:   make(map[string]*fileCache, 1),
			}

			// Now add the path to our cache
			bc.Paths[name] = pc
		}

		if pathRows.Err() != nil {
			pathRows.Close()
			err := pathRows.Err()
			fl.Err(err).Msg("paths-select-rows-done")
			return err
		}

		// Done with the paths
		pathRows.Close()

		// Now we loop through all the paths we just loaded and get all the files for each to cache.
		for _, pc := range bc.Paths {
			fileRows, err := db.Query(ip.ctx, "files-select", pc.id)
			if err != nil {
				fl.Err(err).Msg("files-select")
				return err
			}

			// Loop through our paths
			for fileRows.Next() {
				// Get the values
				//
				// Default query I used for development -
				//
				//   SELECT fid, name, filets, hash, sidets, sidetags, tags FROM files.files WHERE pid = $1 AND enabled
				if err := fileRows.Scan(&inID, &name, &changed, &hash, &sidets, &sideTags, &tgs); err != nil {
					fileRows.Close()
					fl.Err(err).Msg("files-select-rows-scan")
					return err
				}

				// Fix our tags
				sideTags = sideTags.Fix()
				tgs = tgs.Fix()

				// Create our file cache
				fc := &fileCache{
					id:     inID,
					Name:   name,
					Hash:   hash,
					FileTS: changed,
					SideTS: sidets,
					SideTG: sideTags.Copy(),
					CTags:  tgs.Copy(),
				}

				pc.Files[name] = fc
			}

			if fileRows.Err() != nil {
				fileRows.Close()
				err := fileRows.Err()
				fl.Err(err).Msg("files-select-rows-done")
				return err
			}

			// Done with the files
			fileRows.Close()
		}
	}

	fl.Debug().Msg("loaded")

	return nil
} // }}}

// func ImageProc.setupDB {{{

// This creates all prepared statements, and if everything goes OK replaces ip.db with this provided db.
func (ip *ImageProc) setupDB(co *conf, db *pgx.Conn) error {
	fl := ip.l.With().Str("func", "setupDB").Str("db", co.Database).Logger()

	// No using the database after a shutdown.
	if atomic.LoadUint32(&ip.closed) == 1 {
		fl.Debug().Msg("called after shutdown")
		return types.ErrShutdown
	}

	queries := co.Queries

	// Lets prepare all our statements
	if _, err := db.Prepare(ip.ctx, "paths-select", queries.PathsSelect); err != nil {
		fl.Err(err).Msg("paths-select")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "paths-insert", queries.PathsInsert); err != nil {
		fl.Err(err).Msg("paths-insert")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "paths-update", queries.PathsUpdate); err != nil {
		fl.Err(err).Msg("paths-update")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "paths-disable", queries.PathsDisable); err != nil {
		fl.Err(err).Msg("paths-disable")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "files-select", queries.FilesSelect); err != nil {
		fl.Err(err).Msg("files-select")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "files-insert", queries.FilesInsert); err != nil {
		fl.Err(err).Msg("files-insert")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "files-update", queries.FilesUpdate); err != nil {
		fl.Err(err).Msg("files-update")
		return err
	}

	if _, err := db.Prepare(ip.ctx, "files-disable", queries.FilesDisable); err != nil {
		fl.Err(err).Msg("files-disable")
		return err
	}

	fl.Debug().Msg("prepared")

	return nil
} // }}}

// func ImageProc.getDB {{{

// Returns the current database pool.
//
// Loads it from an atomic value so that it can be replaced while running without causing issues.
func (ip *ImageProc) getDB() (*pgxpool.Pool, error) {
	fl := ip.l.With().Str("func", "getDB").Logger()

	// No using the database after a shutdown.
	if atomic.LoadUint32(&ip.closed) == 1 {
		fl.Debug().Msg("called after shutdown")
		return nil, types.ErrShutdown
	}

	db, ok := ip.db.Load().(*pgxpool.Pool)
	if !ok {
		err := errors.New("Not a pool")
		fl.Warn().Err(err).Send()
		return nil, err
	}

	return db, nil
} // }}}

// func ImageProc.checkAll {{{

func (ip *ImageProc) checkAll() {
	fl := ip.l.With().Str("func", "checkAll").Logger()

	// Get the cache.
	ca := ip.ca

	ca.cMut.Lock()
	defer ca.cMut.Unlock()

	for _, bc := range ca.bases {
		fl.Debug().Int("base", bc.Base).Send()

		// Check the base in its own goroutine.
		go ip.checkBase(bc)
	}

	return
} // }}}

// func ImageProc.getBaseCache {{{

// This gets (or adds if not already there) a baseCache for the specific Base.
//
// This assumes you already have a lock on the cache passed in.
func (ip *ImageProc) getBaseCache(cb *confBase, ca *cache) *baseCache {
	fl := ip.l.With().Str("func", "addBaseCache").Logger()

	if ca == nil || cb == nil {
		fl.Warn().Msg("Missing cb or ca")
		return nil
	}

	// Is the cache already there?
	if bc, ok := ca.bases[cb.Base]; ok {
		return bc
	}

	// Base is not there, so lets add it.
	bc := &baseCache{
		Base:  cb.Base,
		Tags:  cb.Tags,
		path:  cb.Path,
		Paths: make(map[string]*pathCache, 1),
	}

	bc.bfs = os.DirFS(cb.Path)

	// Add to the cache.
	ca.bases[bc.Base] = bc

	fl.Debug().Int("base", bc.Base).Msg("Added base")

	return bc
} // }}}

// func ImageProc.makeCheckIntervals {{{

func (ip *ImageProc) makeCheckIntervals() []checkInterval {
	fl := ip.l.With().Str("func", "makeCheckIntervals").Logger()
	now := time.Now()

	// As we support multiple bases, with each being able to have its own check interval, we need
	// a way to check them independently of each other.
	//
	// So we count how many we have, and create an array that lets us know the next time we need to activate
	// an interval check.
	co := ip.getConf()

	// We create an array of check intervals to know what bases to run at what times
	checks := make([]checkInterval, 0, len(co.Bases))

	for _, bc := range co.Bases {
		added := false
		// Is there already a checkInterval with the same duration?
		for _, ci := range checks {
			if ci.checkInt == bc.CheckInt {
				// Yep, same duration so just add our base to it
				ci.bases = append(ci.bases, bc.Base)
				added = true
				break
			}
		}

		if added {
			continue
		}

		// No existing duration match, so create a new one and add it.
		ci := checkInterval{
			checkInt: bc.CheckInt,
		}

		ci.bases = append(ci.bases, bc.Base)
		checks = append(checks, ci)
	}

	// Now set the initial times.
	for i, _ := range checks {
		checks[i].nextRun = now.Add(checks[i].checkInt)
		checks[i].nextDur = checks[i].nextRun.Sub(now)
	}

	sort.Slice(checks, func(i, j int) bool { return checks[i].nextDur < checks[j].nextDur })
	fl.Debug().Time("ntime", checks[0].nextRun).Int("nbase", checks[0].bases[0]).Send()

	return checks
} // }}}

// func ImageProc.setCheckIntervals {{{

func (ip *ImageProc) setCheckIntervals(checks []checkInterval) []checkInterval {
	fl := ip.l.With().Str("func", "setCheckIntervals").Logger()
	now := time.Now()

	// In general, only the first one should ever need to be updated
	if now.After(checks[0].nextRun) {
		checks[0].nextRun = now.Add(checks[0].checkInt)
		checks[0].nextDur = checks[0].nextRun.Sub(now)
	}

	// Now we checked the first above, but it is very possible for two checks needing to fire at the same time.
	//
	// Think of a situation where one is every 5 minutes, and another is every 2 minutes.
	// When the 10 minute check should run they both need to run, so we handle that here.
	for i := 1; i < len(checks); i++ {
		if now.After(checks[i].nextRun) {
			// Looks like this one could have been skipped.
			//
			// So we update it to basically fire right away.
			checks[i].nextRun = now.Add(time.Millisecond)
			checks[i].nextDur = time.Millisecond
			continue
		}

		// It hasn't run yet, but just update its duration, as that keeps shrinking
		checks[i].nextDur = checks[i].nextRun.Sub(now)
	}

	sort.Slice(checks, func(i, j int) bool { return checks[i].nextDur < checks[j].nextDur })
	fl.Debug().Time("ntime", checks[0].nextRun).Int("nbase", checks[0].bases[0]).Send()

	return checks
} // }}}

// func ImageProc.loopy {{{

// Handles our basic background tasks.
func (ip *ImageProc) loopy() {
	fl := ip.l.With().Str("func", "loopy").Logger()

	// Default the base tick to every 5 minutes.
	baseTick := time.NewTicker(5 * time.Minute)
	defer baseTick.Stop()

	ctx := ip.ctx

	// Get the initial checks
	checks := ip.makeCheckIntervals()

	// Lets change the baseTick to the first check we need.
	baseTick.Reset(checks[0].nextDur)

	for {
		select {
		case <-baseTick.C:
			// Get the cache
			ca := ip.ca

			// Temporary lock
			ca.cMut.Lock()
			for _, id := range checks[0].bases {
				fl.Debug().Int("base", id).Msg("baseTick")
				go ip.checkBase(ca.bases[id])

			}
			ca.cMut.Unlock()

			// Update our checks
			checks = ip.setCheckIntervals(checks)

			// And our baseTick
			baseTick.Reset(checks[0].nextDur)
			fl.Debug().Dur("baseTick", checks[0].nextDur).Msg("next tick")
		case _, ok := <-ctx.Done():
			if !ok {
				ip.close()
				return
			}
		}
	}
} // }}}

// func ImageProc.close {{{

// Stops all background processing and disconnects from the database.
//
// This can block as it waits on the database to close.
func (ip *ImageProc) close() {
	fl := ip.l.With().Str("func", "close").Logger()

	// Set closed
	if !atomic.CompareAndSwapUint32(&ip.closed, 0, 1) {
		fl.Info().Msg("already closed")
		return
	}

	// Shutting down, so get the database to shut it down.
	if db, err := ip.getDB(); err == nil {
		// Shutdown the database before we return.
		db.Close()
	}

	fl.Info().Msg("closed")
} // }}}
