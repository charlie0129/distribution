package bufferedpipe

import (
	"errors"
	"sync"
)

// ErrClosedPipe is the error used for read or write operations on a closed pipe.
var ErrClosedPipe = errors.New("bufpipe: read/write on closed pipe")

// BufferedMultiReadPipe is a buffered pipe that can be written or read by multiple clients.
// All writes to the pipe are buffered in memory or on disk, depending on the Buffer implementation.
// All reads are synchronous, meaning that a read will block until data is available. The key
// feature is that the PipeReader which came later can read all the data that was written before it.
//
// The zero value is not usable. Use New to create a BufferedMultiReadPipe.
type BufferedMultiReadPipe struct {
	p *pipe
}

// Reader returns a new PipeReader that can be used to read from the pipe.
// Each call to Reader returns a new reader that can read all the data written to the pipe,
// even if the data was written before the reader was created.
func (p *BufferedMultiReadPipe) Reader() *PipeReader {
	return &PipeReader{pipe: p.p}
}

// Writer returns a PipeWriter that can be used to write to the pipe.
// All writers append to the same pipe, and the data is read by all readers.
func (p *BufferedMultiReadPipe) Writer() *PipeWriter {
	return &PipeWriter{pipe: p.p}
}

type pipe struct {
	cond       *sync.Cond
	buf        Buffer
	rerr, werr error
}

func New(buf Buffer) *BufferedMultiReadPipe {
	p := &pipe{
		buf:  buf,
		cond: sync.NewCond(new(sync.Mutex)),
	}
	return &BufferedMultiReadPipe{p: p}
}
