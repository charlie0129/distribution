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

	"github.com/distribution/reference"
	"github.com/docker/distribution"
	"github.com/docker/distribution/bufferedpipe"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/proxy/scheduler"
	"github.com/opencontainers/go-digest"
)

type inflightBuffer struct {
	wg   *sync.WaitGroup
	desc distribution.Descriptor
	pipe *bufferedpipe.BufferedMultiReadPipe
}

type proxyBlobStore struct {
	localStore     distribution.BlobStore
	remoteStore    distribution.BlobService
	scheduler      *scheduler.TTLExpirationScheduler
	repositoryName reference.Named
	authChallenger authChallenger
}

var _ distribution.BlobStore = &proxyBlobStore{}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]*inflightBuffer)

// mu protects inflight
var mu sync.Mutex

func setResponseHeaders(w http.ResponseWriter, length int64, mediaType string, digest digest.Digest) {
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", digest.String())
	w.Header().Set("Etag", digest.String())
}

func (pbs *proxyBlobStore) downloadFromRemoteStore(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor, writer io.Writer) error {
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

	return nil
}

func (pbs *proxyBlobStore) serveLocal(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	localDesc, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		// Stat can report a zero sized file here if it's checked between creation
		// and population.  Return nil error, and continue
		return false, nil
	}

	if err == nil {
		proxyMetrics.BlobPush(uint64(localDesc.Size))
		return true, pbs.localStore.ServeBlob(ctx, w, r, dgst)
	}

	return false, nil

}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	logger := dcontext.GetLogger(ctx)

	served, err := pbs.serveLocal(ctx, w, r, dgst)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error serving blob from local storage: %s", err.Error())
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

		logger.Infof("Blob already downloading. Following the previous download: %s", dgst)

		proxyMetrics.BlobPush(uint64(desc.Size))

		setResponseHeaders(w, desc.Size, desc.MediaType, dgst)

		_, err = io.CopyN(w, reader, desc.Size)
		if err != nil {
			return err
		}

		return err
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
	setResponseHeaders(w, desc.Size, desc.MediaType, dgst)

	inflightBuf = &inflightBuffer{
		desc: desc,
		pipe: pipe,
	}

	inflight[dgst] = inflightBuf
	mu.Unlock()

	// Start downloading the blob. In a separate goroutine so we can serve the blob while it's downloading.
	go func() {
		var err error
		pipeWriter := pipe.Writer()

		defer func() {
			// Remove the inflight buffer after the download is complete.
			mu.Lock()
			err = pipeWriter.Close()
			if err != nil {
				logger.Errorf("Error closing writer: %s", err)
			}
			// Delete buffer.
			err = fbuf.Close()
			if err != nil {
				logger.Errorf("Error closing file backed buffer: %s", err)
			}
			delete(inflight, dgst)
			mu.Unlock()
		}()

		localWriter, err := pbs.localStore.Create(context.Background())
		if err != nil {
			pipeWriter.CloseWithError(err)
			logger.Errorf("Error creating localStore: %s", err)
			return
		}
		defer localWriter.Cancel(context.Background())

		logger.Infof("Downloading blob %s", dgst)

		// Write to buffer and local store.
		multiWriter := io.MultiWriter(pipeWriter, localWriter)

		// Make the download ctx independent of the request ctx so we continue downloading even if the request is cancelled.
		// TODO: we can let the ctx follow the last cancelled request ctx.
		err = pbs.downloadFromRemoteStore(context.Background(), dgst, desc, multiWriter)
		if err != nil {
			pipeWriter.CloseWithError(err)
			logger.Errorf("Error downloading blob %s: %s", dgst, err)
			return
		}

		logger.Infof("Downloaded blob %s and committing", dgst)

		// Everything is fine. Commit the blob.
		_, err = localWriter.Commit(context.Background(), desc)
		if err != nil {
			pipeWriter.CloseWithError(err)
			logger.Errorf("Error committing blob %s: %s", dgst, err)
			return
		}

		blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
		if err != nil {
			logger.Errorf("Error creating reference: %s", err)
			return
		}

		err = pbs.scheduler.AddBlob(blobRef, repositoryTTL)
		if err != nil {
			logger.Errorf("Error adding blob to scheduler: %s", err)
			return
		}
	}()

	proxyMetrics.BlobPush(uint64(desc.Size))
	// This serves the blob that is downloading in the goroutine started above.
	reader := pipe.Reader()
	_, err = io.CopyN(w, reader, desc.Size)
	if err != nil {
		return err
	}

	return nil
}

func (pbs *proxyBlobStore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	desc, err := pbs.localStore.Stat(ctx, dgst)
	if err == nil {
		return desc, err
	}

	if err != distribution.ErrBlobUnknown {
		return distribution.Descriptor{}, err
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return distribution.Descriptor{}, err
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
func (pbs *proxyBlobStore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Open(ctx context.Context, dgst digest.Digest) (io.ReadSeekCloser, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return distribution.ErrUnsupported
}
