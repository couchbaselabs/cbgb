package cbgb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
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

func (bs *BucketSettings) Copy() *BucketSettings {
	return &BucketSettings{
		NumPartitions:    bs.NumPartitions,
		PasswordHashFunc: bs.PasswordHashFunc,
		PasswordHash:     bs.PasswordHash,
		PasswordSalt:     bs.PasswordSalt,
		QuotaBytes:       bs.QuotaBytes,
		MemoryOnly:       bs.MemoryOnly,
	}
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
	b, err := ioutil.ReadFile(path.Join(bucketDir, "settings.json"))
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
	fname := path.Join(bucketDir, "settings.json")
	fnameNew := path.Join(bucketDir, "settings.json.new")
	fnameOld := path.Join(bucketDir, "settings.json.old")
	if err = ioutil.WriteFile(fnameNew, j, 0600); err != nil {
		return err
	}
	os.Rename(fname, fnameOld)
	return os.Rename(fnameNew, fname)
}
