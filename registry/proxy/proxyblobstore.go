package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/internal/bufferedpipe"
	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/registry/proxy/scheduler"
	"github.com/distribution/reference"
)

type inflightBuffer struct {
	wg   *sync.WaitGroup
	desc v1.Descriptor
	pipe *bufferedpipe.BufferedMultiReadPipe
}

type proxyBlobStore struct {
	localStore     distribution.BlobStore
	remoteStore    distribution.BlobService
	scheduler      *scheduler.TTLExpirationScheduler
	ttl            *time.Duration
	repositoryName reference.Named
	authChallenger authChallenger
}

var _ distribution.BlobStore = &proxyBlobStore{}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]*inflightBuffer)

// mu protects inflight
var mu sync.Mutex

func setResponseHeaders(h http.Header, length int64, mediaType string, digest digest.Digest) {
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	h.Set("Content-Type", mediaType)
	h.Set("Docker-Content-Digest", digest.String())
	h.Set("Etag", digest.String())
}

func (pbs *proxyBlobStore) copyContent(ctx context.Context, dgst digest.Digest, writer io.Writer, desc v1.Descriptor) error {
	remoteReader, err := pbs.remoteStore.Open(ctx, dgst)
	if err != nil {
		return err
	}

	defer remoteReader.Close()

	_, err = io.CopyN(writer, remoteReader, desc.Size)
	if err != nil {
		return err
	}

	proxyMetrics.BlobPull(uint64(desc.Size))
	proxyMetrics.BlobPush(uint64(desc.Size), false)

	return nil
}

func (pbs *proxyBlobStore) serveLocal(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	localDesc, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		// Stat can report a zero sized file here if it's checked between creation
		// and population.  Return nil error, and continue
		return false, nil
	}

	proxyMetrics.BlobPush(uint64(localDesc.Size), true)
	return true, pbs.localStore.ServeBlob(ctx, w, r, dgst)
}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	logger := dcontext.GetLogger(ctx)

	served, err := pbs.serveLocal(ctx, w, r, dgst)
	if err != nil {
		logger.Errorf("Error serving blob from local storage: %s", err.Error())
		return err
	}

	if served {
		return nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return err
	}

	mu.Lock()
	inflightBuf, ok := inflight[dgst]
	if ok {
		mu.Unlock()

		desc := inflightBuf.desc
		// This reader catches up and follows the progress of the previous download job.
		reader := inflightBuf.pipe.Reader()

		logger.Infof("Blob %s already downloading. Following the previous download.", dgst)

		// Following other downloads is considered a hit because we are not making a new request to the remote.
		proxyMetrics.BlobPush(uint64(desc.Size), true)

		setResponseHeaders(w.Header(), desc.Size, desc.MediaType, dgst)

		_, err = io.CopyN(w, reader, desc.Size)
		if err != nil {
			return err
		}

		return nil
	}

	// Create a new inflight buffer. This file backed buffer will be used to store the blob while it's downloading.
	// The actual file will be deleted after the download is complete, by fbuf.Close().
	fbuf, err := bufferedpipe.NewFileBackedBuffer(path.Join(os.TempDir(), dgst.Hex()))
	if err != nil {
		mu.Unlock()
		return fmt.Errorf("failed to create file backed buffer: %v", err)
	}

	pipe := bufferedpipe.New(fbuf)

	desc, err := pbs.remoteStore.Stat(ctx, dgst)
	if err != nil {
		mu.Unlock()
		return err
	}
	setResponseHeaders(w.Header(), desc.Size, desc.MediaType, dgst)

	inflightBuf = &inflightBuffer{
		desc: desc,
		pipe: pipe,
	}

	inflight[dgst] = inflightBuf
	mu.Unlock()

	// Start downloading the blob. In a separate goroutine so we can serve the blob while it's downloading.
	go func() {
		writer := pipe.Writer()

		defer func() {
			// Remove the inflight buffer after the download is complete.
			mu.Lock()
			writer.Close()
			// Delete buffer.
			fbuf.Close()
			delete(inflight, dgst)
			mu.Unlock()
		}()

		bw, err := pbs.localStore.Create(ctx)
		if err != nil {
			writer.CloseWithError(err)
			logger.Errorf("Error creating localStore: %s", err)
			return
		}
		defer bw.Close()

		logger.Infof("Downloading blob %s", dgst)

		// Write to buffer and local store.
		multiWriter := io.MultiWriter(writer, bw)

		// Make the download ctx independent of the request ctx so we continue downloading even if the request is cancelled.
		// TODO: we can let the ctx follow the last cancelled request ctx.
		err = pbs.copyContent(context.Background(), dgst, multiWriter, desc)
		if err != nil {
			writer.CloseWithError(err)
			logger.Errorf("Error downloading blob %s: %s", dgst, err)
			return
		}

		logger.Infof("Downloaded blob %s and committing", dgst)

		// Everything is fine. Commit the blob.
		_, err = bw.Commit(ctx, desc)
		if err != nil {
			writer.CloseWithError(err)
			logger.Errorf("Error committing blob %s: %s", dgst, err)
			return
		}
	}()

	// This serves the blob that is downloading in the goroutine started above.
	reader := pipe.Reader()
	logger.Infof("Following the downloader of blob %s", dgst)
	_, err = io.CopyN(w, reader, desc.Size)
	if err != nil {
		return err
	}

	blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
	if err != nil {
		logger.Errorf("Error creating reference: %s", err)
		return err
	}

	if pbs.scheduler != nil && pbs.ttl != nil {
		if err := pbs.scheduler.AddBlob(blobRef, *pbs.ttl); err != nil {
			logger.Errorf("Error adding blob: %s", err)
			return err
		}
	}

	return nil
}

func (pbs *proxyBlobStore) Stat(ctx context.Context, dgst digest.Digest) (v1.Descriptor, error) {
	desc, err := pbs.localStore.Stat(ctx, dgst)
	if err == nil {
		return desc, err
	}

	if err != distribution.ErrBlobUnknown {
		return v1.Descriptor{}, err
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return v1.Descriptor{}, err
	}

	return pbs.remoteStore.Stat(ctx, dgst)
}

func (pbs *proxyBlobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	blob, err := pbs.localStore.Get(ctx, dgst)
	if err == nil {
		return blob, nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return []byte{}, err
	}

	blob, err = pbs.remoteStore.Get(ctx, dgst)
	if err != nil {
		return []byte{}, err
	}

	_, err = pbs.localStore.Put(ctx, "", blob)
	if err != nil {
		return []byte{}, err
	}
	return blob, nil
}

// Unsupported functions
func (pbs *proxyBlobStore) Put(ctx context.Context, mediaType string, p []byte) (v1.Descriptor, error) {
	return v1.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest) (v1.Descriptor, error) {
	return v1.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Open(ctx context.Context, dgst digest.Digest) (io.ReadSeekCloser, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return distribution.ErrUnsupported
}
