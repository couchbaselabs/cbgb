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
	fs   *FileService
	path string
	mode int
}

// Open a FileLike thing that works within this FileService
func (fs *FileService) OpenFile(path string, mode int) (FileLike, error) {
	rv := &fileLike{fs, path, mode}
	err := fs.Do(rv.path, rv.mode, func(*os.File) error { return nil })
	// Clear out bits that only make sense the first time
	// you open something.
	rv.mode = rv.mode &^ (os.O_EXCL | os.O_APPEND | os.O_TRUNC)
	return rv, err
}

func (f *fileLike) Close() error {
	return nil
}

// Stat the underlying path.
func (f *fileLike) Stat() (os.FileInfo, error) {
	return os.Lstat(f.path)
}

func (f *fileLike) ReadAt(p []byte, off int64) (n int, err error) {
	if f.mode&os.O_WRONLY == os.O_WRONLY {
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
		n, err = file.WriteAt(p, off)
		return err
	})
	return
}
