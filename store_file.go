package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/steveyen/gkvlite"
)

// A bucketstore may have multiple bucketstorefiles, such as during
// compaction.
type bucketstorefile struct {
	path  string
	file  FileLike
	store *gkvlite.Store
	lock  sync.Mutex
	purge bool // When true, purge file when GC finalized.
	stats *BucketStoreStats
}

func NewBucketStoreFile(path string, file FileLike,
	stats *BucketStoreStats) *bucketstorefile {
	res := &bucketstorefile{
		path:  path,
		file:  file,
		stats: stats,
	}
	runtime.SetFinalizer(res, finalizeBucketStoreFile)
	return res
}

func finalizeBucketStoreFile(bsf *bucketstorefile) {
	if bsf.purge {
		bsf.purge = false
		os.Remove(bsf.path)
	}
}

func (bsf *bucketstorefile) apply(fun func()) {
	bsf.lock.Lock()
	defer bsf.lock.Unlock()
	fun()
}

// The following bucketstore methods implement the gkvlite.StoreFile
// interface: ReadAt(), WriteAt(), Stat().

func (bsf *bucketstorefile) ReadAt(p []byte, off int64) (n int, err error) {
	bsf.apply(func() {
		atomic.AddInt64(&bsf.stats.Reads, 1)
		n, err = bsf.file.ReadAt(p, off)
		if err != nil {
			atomic.AddInt64(&bsf.stats.ReadErrors, 1)
		}
		atomic.AddInt64(&bsf.stats.ReadBytes, int64(n))
	})
	return n, err
}

func (bsf *bucketstorefile) WriteAt(p []byte, off int64) (n int, err error) {
	bsf.apply(func() {
		if bsf.purge {
			err = fmt.Errorf("WriteAt to purgable bucketstorefile: %v",
				bsf.path)
			return
		}
		atomic.AddInt64(&bsf.stats.Writes, 1)
		n, err = bsf.file.WriteAt(p, off)
		if err != nil {
			atomic.AddInt64(&bsf.stats.WriteErrors, 1)
		}
		atomic.AddInt64(&bsf.stats.WriteBytes, int64(n))
	})
	return n, err
}

func (bsf *bucketstorefile) Stat() (fi os.FileInfo, err error) {
	bsf.apply(func() {
		atomic.AddInt64(&bsf.stats.Stats, 1)
		fi, err = bsf.file.Stat()
		if err != nil {
			atomic.AddInt64(&bsf.stats.StatErrors, 1)
		}
	})
	return fi, err
}

// Find the highest version-numbered store files in a bucket directory.
func latestStoreFileNames(dirForBucket string, storesPerBucket int,
	suffix string) (res []string, err error) {
	res = make([]string, storesPerBucket)
	for i := 0; i < storesPerBucket; i++ {
		prefix := strconv.FormatInt(int64(i), 10)
		res[i], err = latestStoreFileName(dirForBucket, prefix, suffix)
		if err != nil {
			return nil, err
		}
	}
	return res, err
}

func latestStoreFileName(dirForBucket string, prefix string, suffix string) (
	string, error) {
	fileInfos, err := ioutil.ReadDir(dirForBucket)
	if err != nil {
		return "", err
	}
	latestVer := 0
	latestName := makeStoreFileName(prefix, latestVer, suffix)
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		prefixCur, ver, err :=
			parseStoreFileName(fileInfo.Name(), suffix)
		if err != nil {
			continue
		}
		if prefixCur != prefix {
			continue
		}
		if latestVer < ver {
			latestVer = ver
			latestName = fileInfo.Name()
		}
	}
	return latestName, nil
}

// The store files follow a "PREFIX-VER.SUFFIX" naming pattern,
// such as "0-0.store".
func makeStoreFileName(prefix string, ver int, suffix string) string {
	return fmt.Sprintf("%v-%v.%v", prefix, ver, suffix)
}

func parseStoreFileName(fileName string, suffix string) (
	prefix string, ver int, err error) {
	if !strings.HasSuffix(fileName, "."+suffix) {
		return "", -1, fmt.Errorf("missing suffix: %v in filename: %v",
			suffix, fileName)
	}
	base := fileName[0 : len(fileName)-(1+len(suffix))]
	parts := strings.Split(base, "-")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return "", -1, fmt.Errorf("not a store filename: %v", fileName)
	}
	ver, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", -1, err
	}
	return parts[0], ver, nil
}
