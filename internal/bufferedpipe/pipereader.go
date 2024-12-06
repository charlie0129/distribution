package bufferedpipe

import (
	"io"
)

// A PipeReader is the read half of a pipe.
type PipeReader struct {
	off int64
	*pipe
}

// Read implements the standard Read interface: it reads data from the pipe,
// reading from the internal buffer, otherwise blocking until a writer arrives
// or the write end is closed. If the write end is closed with an error, that
// error is returned as err; otherwise err is io.EOF.
func (r *PipeReader) Read(p []byte) (int, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

RETRY:
	n, err := r.buf.ReadAt(p, r.off)
	r.off += int64(n)
	// If not closed and no read, wait for writing.
	if err == io.EOF && r.rerr == nil && n == 0 {
		r.cond.Wait()
		goto RETRY
	}
	if err == io.EOF {
		return n, r.rerr
	}
	return n, err
}

// Close closes the reader; subsequent writes from the write half of the pipe
// will return error ErrClosedPipe.
func (r *PipeReader) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes to the write half of the
// pipe will return the error err.
func (r *PipeReader) CloseWithError(err error) error {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	if err == nil {
		err = ErrClosedPipe
	}
	r.werr = err
	return nil
}
