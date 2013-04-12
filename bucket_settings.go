package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	MemoryOnly_LEVEL_PERSIST_EVERYTHING = 0

	// A bucket created at this level survives a server restart, but
	// will restart with zero items.  Item ops are not persisted.
	MemoryOnly_LEVEL_PERSIST_METADATA = 1

	// Don't use any files, and bucket disappears at server restart.
	MemoryOnly_LEVEL_PERSIST_NOTHING = 2
)

type BucketSettings struct {
	NumPartitions    int    `json:"numPartitions"`
	PasswordHashFunc string `json:"passwordHashFunc"`
	PasswordHash     string `json:"passwordHash"`
	PasswordSalt     string `json:"passwordSalt"`
	QuotaBytes       int64  `json:"quotaBytes"`
	MemoryOnly       int    `json:"memoryOnly"`
	UUID             string `json:"uuid"`
}

type pwverifier func(salt string, bpass, input []byte) bool

var pwverifiers = map[string]pwverifier{
	"": func(salt string, bpass, input []byte) bool {
		if salt != "" {
			return false
		}
		return bytes.Equal([]byte(bpass), input)
	},
}

func (bs *BucketSettings) Auth(input []byte) bool {
	if bs == nil {
		return false
	}
	fun := pwverifiers[bs.PasswordHashFunc]
	if fun == nil {
		return false
	}
	return fun(bs.PasswordSalt, []byte(bs.PasswordHash), input)
}

func (bs *BucketSettings) Copy() *BucketSettings {
	rv := *bs
	return &rv
}

// Returns a safe subset (no passwords) useful for JSON-ification.
func (bs *BucketSettings) SafeView() map[string]interface{} {
	return map[string]interface{}{
		"numPartitions": bs.NumPartitions,
		"quotaBytes":    bs.QuotaBytes,
		"memoryOnly":    bs.MemoryOnly,
		"uuid":          bs.UUID,
	}
}

func (bs *BucketSettings) load(bucketDir string) (exists bool, err error) {
	b, err := ioutil.ReadFile(filepath.Join(bucketDir, "settings.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, json.Unmarshal(b, bs)
}

func (bs *BucketSettings) save(bucketDir string) error {
	j, err := json.Marshal(bs)
	if err != nil {
		return err
	}
	fname := filepath.Join(bucketDir, "settings.json")
	fnameNew := filepath.Join(bucketDir, "settings.json.new")
	fnameOld := filepath.Join(bucketDir, "settings.json.old")
	if err = ioutil.WriteFile(fnameNew, j, 0600); err != nil {
		return err
	}
	os.Rename(fname, fnameOld)
	return os.Rename(fnameNew, fname)
}
