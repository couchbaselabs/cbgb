package cbgb

import (
	"errors"
	"io"
	"os"
)

var unReadable = errors.New("file is not open for reading")
var unWritable = errors.New("file is not open for writing")

// A FileLike does things kind of like a file.
type FileLike interface {
	io.Closer
	io.ReaderAt
	io.WriterAt

	Stat() (os.FileInfo, error)
}

type fileLike struct {
	fs   *fileService
	path string
	mode int
}

// Open a FileLike thing that works within this fileService
func (fs *fileService) OpenFile(path string, mode int) (FileLike, error) {
	return &fileLike{fs, path, mode}, nil
}

func (f *fileLike) Close() error {
	return nil
}

// Stat the underlying path.
func (f *fileLike) Stat() (os.FileInfo, error) {
	return os.Lstat(f.path)
}

func (f *fileLike) ReadAt(p []byte, off int64) (n int, err error) {
	if f.mode&^(os.O_WRONLY) != 0 {
		return 0, unReadable
	}
	err = f.fs.Do(f.path, f.mode, func(file *os.File) error {
		n, err = file.ReadAt(p, off)
		return err
	})
	return
}

func (f *fileLike) WriteAt(p []byte, off int64) (n int, err error) {
	if f.mode&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, unWritable
	}

	err = f.fs.Do(f.path, f.mode, func(file *os.File) error {
		// Clear out bits that only make sense the first time
		// you open something.
		f.mode = f.mode &^ (os.O_EXCL | os.O_APPEND | os.O_TRUNC)
		n, err = file.WriteAt(p, off)
		return err
	})
	return
}
