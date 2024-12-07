package bufferedpipe

type Buffer interface {
	Write(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
}
