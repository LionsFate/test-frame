package imgproc

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

// const hash bits {{{

// XXX TODO XXX Make the system use these!
// The allowed hash types for hashing files.
var hashes = map[string]int{
	"sha-1":   1,
	"sha-224": 224,
	"sha-256": 256,
	"sha-384": 384,
	"sha-512": 512,
} // }}}

// func ImageProc.getHash {{{

// We allow the hash function to be chosen from the configuration file, or defaulting to SHA-256.
//
// This hash function is not used for cryptography, it is used to name the file in the cache, as duplicate files
// should all have the same hash.
//
// As such, SHA-1 is an option.
//
// Can be useful if you only have a few thousand files and are running on an rpi, odds off collision?
//
func (ip *ImageProc) getHash(hash int) hash.Hash {
	switch hash {
	case 1:
		// This is not secure for cryptography, but if someone wants to use it just to hash file names?
		// We mainly use this hash to eliminate duplicate files in the cache, so not exactly crypto.
		//
		// Its an option if you want it.
		return sha1.New()
	case 224:
		return sha256.New224()
	case 256:
		return sha256.New()
	case 384:
		return sha512.New384()
	case 512:
		return sha512.New()
	default:
		return sha256.New()
	}
} // }}}
