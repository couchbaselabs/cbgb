package main

import (
	"os"
)

type fileRequest struct {
	path  string
	flags int
	f     func(f *os.File) error
	res   chan error
}

type FileService struct {
	reqs chan fileRequest
}

func NewFileService(workers int) *FileService {
	fs := &FileService{make(chan fileRequest, 1000)}
	for i := 0; i < workers; i++ {
		go fs.worker()
	}
	return fs
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
	r := fileRequest{path, flags, fn, make(chan error, 1)}
	f.reqs <- r
	return <-r.res
}

func (f *FileService) Close() error {
	close(f.reqs)
	return nil
}

func serviceFileRequest(r fileRequest) {
	f, err := os.OpenFile(r.path, r.flags, 0666)
	if err == nil {
		defer f.Close()
		err = r.f(f)
	}
	r.res <- err
}

func (f *FileService) worker() {
	for req := range f.reqs {
		serviceFileRequest(req)
	}
}
