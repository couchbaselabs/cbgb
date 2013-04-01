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
