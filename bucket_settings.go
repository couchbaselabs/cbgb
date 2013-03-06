package cbgb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
)

type BucketSettings struct {
	NumPartitions    int    `json:"numPartitions"`
	PasswordHashFunc string `json:"passwordHashFunc"`
	PasswordHash     string `json:"passwordHash"`
	PasswordSalt     string `json:"passwordSalt"`
	QuotaBytes       int    `json:"quotaBytes"`
}

func (bs *BucketSettings) Copy() *BucketSettings {
	return &BucketSettings{
		NumPartitions:    bs.NumPartitions,
		PasswordHashFunc: bs.PasswordHashFunc,
		PasswordHash:     bs.PasswordHash,
		PasswordSalt:     bs.PasswordSalt,
		QuotaBytes:       bs.QuotaBytes,
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
	if err = os.Rename(fnameNew, fname); err != nil {
		return err
	}
	return nil
}
