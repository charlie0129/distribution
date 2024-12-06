package bufferedpipe

import (
	"os"
	"sync"
)

// FileBackedBuffer is a Buffer backed by a file.
type FileBackedBuffer struct {
	f  *os.File
	mu *sync.RWMutex
}

// NewFileBackedBuffer creates a new FileBackedBuffer backed by a file. Be sure to call Close() on the buffer when done.
func NewFileBackedBuffer(filename string) (*FileBackedBuffer, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &FileBackedBuffer{
		f:  f,
		mu: new(sync.RWMutex),
	}, nil
}

func (b *FileBackedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f.Write(p)
}

func (b *FileBackedBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.f.ReadAt(p, off)
}

func (b *FileBackedBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.f.Close()
	if err != nil {
		return err
	}

	return os.Remove(b.f.Name())
}
