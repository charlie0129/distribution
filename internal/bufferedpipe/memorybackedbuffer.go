package bufferedpipe

import (
	"bytes"
	"sync"
)

func NewMemoryBackedBuffer() Buffer {
	return &MemoryBackedBuffer{
		mu: new(sync.RWMutex),
	}
}

type MemoryBackedBuffer struct {
	buf bytes.Buffer
	mu  *sync.RWMutex
}

func (b *MemoryBackedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *MemoryBackedBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return bytes.NewReader(b.buf.Bytes()).ReadAt(p, off)
}
