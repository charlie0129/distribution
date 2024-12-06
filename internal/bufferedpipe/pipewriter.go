package bufferedpipe

import (
	"io"
)

// PipeWriter is the write half of a pipe.
type PipeWriter struct {
	*pipe
}

// Write implements the standard Write interface: it writes data to the internal
// buffer. If the read end is closed with an error, that err is returned as err;
// otherwise err is ErrClosedPipe.
func (w *PipeWriter) Write(data []byte) (int, error) {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()

	if w.werr != nil {
		return 0, w.werr
	}

	n, err := w.buf.Write(data)
	w.cond.Broadcast()
	return n, err
}

// Close closes the writer; subsequent reads from the read half of the pipe will
// return io.EOF once the internal buffer get empty.
func (w *PipeWriter) Close() error {
	return w.CloseWithError(nil)
}

// Close closes the writer; subsequent reads from the read half of the pipe will
// return err once the internal buffer get empty.
func (w *PipeWriter) CloseWithError(err error) error {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()

	if err == nil {
		err = io.EOF
	}
	w.rerr = err
	w.cond.Broadcast()
	return nil
}
