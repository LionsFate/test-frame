package tags

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"strings"
)

// func LoadTagFile {{{

// This returns a Tags for all the files contained within the given file.
// The file format is a UTF-8 text file, one tag per-line.
func LoadTagFile(ffs fs.FS, file string, tm TagManager) (Tags, error) {
	var newTags Tags

	// Now open the sidecar for reading.
	f, err := ffs.Open(file)
	if err != nil {
		return newTags, err
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

			return newTags, fmt.Errorf("read(%s): %w", file, err)
		}

		// Strip any spaces from tag.
		line = strings.TrimSpace(line)

		// Skip empty tags, as well as absurdly long tags (WTH dude?)
		if line == "" || len(line) > 100 {
			continue
		}

		// Get the tag from TagManager.
		tag, err := tm.Get(line)
		if err != nil {
			return newTags, err
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

	return newTags, nil
} // }}}
