package main

import (
	"os"
)

type FileService struct {
	reqs chan bool
}

func NewFileService(concurrency int) *FileService {
	return &FileService{make(chan bool, concurrency)}
}

// Open a FileLike thing that works within this FileService.
func (fs *FileService) OpenFile(path string, mode int) (FileLike, error) {
	rv := &fileLike{fs, path, mode}
	err := fs.Do(rv.path, rv.mode, func(*os.File) error { return nil })
	// Clear out bits that only make sense the first time
	// you open something.
	rv.mode = rv.mode &^ (os.O_EXCL | os.O_APPEND | os.O_TRUNC)
	return rv, err
}

func (f *FileService) Do(path string, flags int, fn func(*os.File) error) error {
	f.reqs <- true
	defer func() { <-f.reqs }()
	file, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	return fn(file)
}

func (f *FileService) Close() error {
	close(f.reqs)
	return nil
}
