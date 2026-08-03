// Multipart upload support for serve s3.
//
// Multipart uploads received by serve s3 are streamed, in part-number order,
// into a single PutStream upload to the underlying Fs, so the whole file is
// never buffered in memory. This implements the gofakes3.MultipartBackend
// interface on s3Backend.
//
// When streaming is disabled (--disable-multipart-streaming) or the Fs has no
// PutStream, ErrMultipartUploadNotSupported is returned so that gofakes3 falls
// back to buffering the parts in memory.

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ncw/swift/v2"
	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// multipartUploadPrefix is prepended to the leaf name of the temporary object
// a streamed multipart upload is written to before it is moved into place.
const multipartUploadPrefix = ".rclone_multipart_upload_"

// multipartUpload tracks one in-flight S3 multipart upload. The parts are
// written, in part-number order, into a single sink: either a pipe feeding a
// background PutStream upload to the underlying Fs, or (when the VFS is
// caching writes) a VFS file handle backed by the cache.
type multipartUpload struct {
	bucket, key string
	fp          string // final object path
	streamFp    string // path the parts are streamed to (fp when the backend uploads atomically)
	meta        map[string]string

	pipeW *io.PipeWriter // streaming sink: parts are piped to the background PutStream
	fh    vfs.Handle     // cached sink: parts are written into the VFS cache

	mu        sync.Mutex
	cond      *sync.Cond     // signalled when buffered shrinks, nextPart advances or the upload closes
	partMD5s  map[int][]byte // raw MD5 sums per part (for the final S3 multipart ETag)
	partSizes map[int]int64  // observed part sizes
	closed    bool

	putCancel   context.CancelFunc // streaming sink: cancels the background PutStream
	putDone     chan struct{}      // streaming sink: closed when the background PutStream returns
	putErr      error              // PutStream result (read only after putDone is closed)
	nextPart    int                // next part number to stream (1-based)
	streamBuf   map[int]*pool.RW   // parts received ahead of nextPart, awaiting their turn
	pumping     bool               // a goroutine is currently writing to the sink
	buffered    int64              // bytes of parts admitted but not yet streamed or released
	bufferLimit int64              // max buffered before parts ahead of nextPart must wait (<= 0 for no limit)
}

// sink returns the writer the in-order part stream is pumped into.
func (up *multipartUpload) sink() io.Writer {
	if up.fh != nil {
		return up.fh
	}
	return up.pipeW
}

// newMultipartUpload allocates an upload struct.
func newMultipartUpload(bucket, key, fp, streamFp string, meta map[string]string, bufferLimit int64) *multipartUpload {
	up := &multipartUpload{
		bucket:      bucket,
		key:         key,
		fp:          fp,
		streamFp:    streamFp,
		meta:        meta,
		partMD5s:    map[int][]byte{},
		partSizes:   map[int]int64{},
		nextPart:    1,
		streamBuf:   map[int]*pool.RW{},
		bufferLimit: bufferLimit,
	}
	up.cond = sync.NewCond(&up.mu)
	return up
}

// loadUpload looks up an in-flight upload by ID.
func (b *s3Backend) loadUpload(uploadID gofakes3.UploadID) (*multipartUpload, error) {
	v, ok := b.multipartUploads.Load(uploadID)
	if !ok {
		return nil, gofakes3.ErrNoSuchUpload
	}
	return v.(*multipartUpload), nil
}

// CreateMultipartUpload begins a new multipart upload.
//
// When the VFS is caching writes (--vfs-cache-mode writes or above) the parts
// are written, in part-number order, to a temporary file in the VFS cache
// which is renamed into place on completion and uploaded by the VFS
// write-back, exactly like a plain PutObject. This needs no PutStream support
// from the Fs, only a server-side move or copy for the rename.
//
// Otherwise the parts are streamed, in part-number order, into a single
// PutStream upload to the underlying Fs. Backends that upload atomically
// (PartialUploads=false) are streamed straight to the final object. An
// aborted or failed upload never makes a partial object visible or disturbs
// a pre-existing one. Backends where a partial upload is visible
// (PartialUploads=true) are instead streamed to a temporary object that is
// moved into place, server-side, on completion, giving the same atomic
// behaviour.
//
// If neither path is usable (streaming disabled with
// --disable-multipart-streaming, no PutStream, or a non-atomic Fs that can't
// move/copy objects server-side), ErrMultipartUploadNotSupported is returned
// so that gofakes3 falls back to buffering the whole upload in memory; a
// one-off NOTICE warns about the memory use.
func (b *s3Backend) CreateMultipartUpload(ctx context.Context, bucketName, objectName string, meta map[string]string) (gofakes3.UploadID, error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return "", err
	}
	if _, err := _vfs.Stat(bucketName); err != nil {
		return "", gofakes3.BucketNotFound(bucketName)
	}

	f := _vfs.Fs()
	features := f.Features()
	// Write via the VFS cache when it is caching writes, so multipart uploads
	// and plain PUTs to the same key go through the same cache item and the
	// upload behaves like any other cached write.
	useCache := _vfs.Opt.CacheMode >= vfscommon.CacheModeWrites && operations.CanServerSideMove(f)
	if !useCache {
		if reason := b.noStreamingReason(f); reason != "" {
			b.warnInMemoryOnce.Do(func() {
				fs.Logf(nil, "serve s3: buffering multipart uploads in memory because %s - this may use a lot of memory - using --vfs-cache-mode writes would buffer them to disk in the VFS cache instead", reason)
			})
			return "", gofakes3.ErrMultipartUploadNotSupported
		}
	}

	fp, err := bucketObjectPath(bucketName, objectName)
	if err != nil {
		return "", err
	}
	objectDir := path.Dir(fp)
	if objectDir != "." {
		if err := mkdirRecursive(objectDir, _vfs); err != nil {
			return "", err
		}
	}

	uploadID := gofakes3.UploadID(uuid.New().String())
	streamFp := fp
	// Write to a temporary object unless the parts stream straight to an
	// atomic backend - in the cache an unfinished upload is always visible.
	if useCache || features.PartialUploads {
		streamFp = path.Join(objectDir, multipartUploadPrefix+string(uploadID))
	}

	up := newMultipartUpload(bucketName, objectName, fp, streamFp, meta, int64(b.s.opt.MultipartStreamingBufferLimit))

	if useCache {
		fh, err := _vfs.Create(streamFp)
		if err != nil {
			return "", err
		}
		up.fh = fh
	} else {
		src := object.NewStaticObjectInfo(streamFp, time.Now(), -1, true, nil, f)
		pr, pw := io.Pipe()
		// Use a context that outlives this request (it's cancelled on abort) but
		// keeps its values.
		putCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		up.pipeW = pw
		up.putCancel = cancel
		up.putDone = make(chan struct{})
		go func() {
			_, err := features.PutStream(putCtx, pr, src)
			up.putErr = err
			_ = pr.CloseWithError(err)
			close(up.putDone)
		}()
	}

	b.multipartUploads.Store(uploadID, up)
	return uploadID, nil
}

// noStreamingReason returns a non-empty reason why streamed multipart uploads
// can't be used for f, or "" if they can.
func (b *s3Backend) noStreamingReason(f fs.Fs) string {
	switch {
	case b.s.opt.DisableMultipartStreaming:
		return "--disable-multipart-streaming is set"
	case f.Features().PutStream == nil:
		return "this backend doesn't support streaming uploads"
	case f.Features().PartialUploads && !operations.CanServerSideMove(f):
		return "this backend can't upload atomically and has no server-side move or copy"
	default:
		return ""
	}
}

// UploadPart writes a single part from the S3 client into the streaming upload.
func (b *s3Backend) UploadPart(ctx context.Context, bucketName, objectName string, uploadID gofakes3.UploadID, partNumber int, contentLength int64, body io.Reader) (string, error) {
	up, err := b.loadUpload(uploadID)
	if err != nil {
		return "", err
	}

	// Wait until there is room to buffer this part, bounding the memory a
	// client which uploads faster than the backend drains can consume.
	if err := up.waitForTurn(partNumber, contentLength); err != nil {
		return "", err
	}

	// Buffer the part in a pool-backed RW so we can MD5 it (for the ETag) and
	// stream it once it is this part's turn.
	rw := multipart.NewRW().Reserve(contentLength)
	hasher := md5.New()
	n, err := io.Copy(rw, io.TeeReader(body, hasher))
	if err != nil {
		_ = rw.Close()
		up.release(contentLength)
		return "", err
	}
	if n != contentLength {
		_ = rw.Close()
		up.release(contentLength)
		return "", gofakes3.ErrIncompleteBody
	}
	md5Sum := hasher.Sum(nil)
	etag := fmt.Sprintf("%q", hex.EncodeToString(md5Sum))

	if err := up.streamPart(partNumber, n, md5Sum, rw); err != nil {
		return "", err
	}
	return etag, nil
}

// waitForTurn blocks until size bytes can be admitted to the reorder buffer,
// then reserves them, bounding the memory an upload can consume when the
// client sends parts faster than the backend drains them.
//
// The next part the stream needs (and any retry of an earlier one) is always
// admitted so the pipe can keep draining; so is a single part bigger than the
// limit when the buffer is empty, to guarantee progress. Reserved bytes are
// returned with release, or by the pump as the part is streamed.
func (up *multipartUpload) waitForTurn(partNumber int, size int64) error {
	up.mu.Lock()
	defer up.mu.Unlock()
	for {
		if up.closed {
			return gofakes3.ErrNoSuchUpload
		}
		if up.bufferLimit <= 0 || partNumber <= up.nextPart || up.buffered == 0 || up.buffered+size <= up.bufferLimit {
			up.buffered += size
			return nil
		}
		up.cond.Wait()
	}
}

// release returns size bytes reserved by waitForTurn to the reorder buffer
// budget and wakes any parts waiting for room.
func (up *multipartUpload) release(size int64) {
	up.mu.Lock()
	up.buffered -= size
	up.cond.Broadcast()
	up.mu.Unlock()
}

// streamPart records a part and streams the parts into the pipe in order.
//
// Parts must be uploaded in ascending, contiguous part-number order. A part
// that arrives ahead of the next expected one is buffered until its turn; the
// parts are then pumped into the pipe in order. Whichever goroutine finds the
// next part available does the pumping, so concurrent (but in-order) clients
// are tolerated, with the buffering bounded by waitForTurn.
//
// A part number may be uploaded more than once - typically a client retrying
// after its request timed out, but real S3 also allows replacing a part. If
// the earlier copy is still buffered it is replaced (last write wins). If it
// has already been streamed it can't be replaced: an identical re-upload is
// accepted idempotently (the data is already in the stream) and a different
// one is rejected.
func (up *multipartUpload) streamPart(partNumber int, size int64, md5Sum []byte, rw *pool.RW) error {
	up.mu.Lock()
	if up.closed {
		// The upload was aborted or completed while the part body was
		// being received.
		up.buffered -= size
		up.cond.Broadcast()
		up.mu.Unlock()
		_ = rw.Close()
		return gofakes3.ErrNoSuchUpload
	}
	if oldMD5, exists := up.partMD5s[partNumber]; exists {
		if old, buffered := up.streamBuf[partNumber]; buffered {
			_ = old.Close()
			up.buffered -= up.partSizes[partNumber]
			up.cond.Broadcast()
		} else {
			// Already streamed (or streaming right now): the stream can't be
			// rewritten, so accept an identical part and reject the rest.
			same := bytes.Equal(md5Sum, oldMD5) && size == up.partSizes[partNumber]
			up.buffered -= size
			up.cond.Broadcast()
			up.mu.Unlock()
			_ = rw.Close()
			if !same {
				return gofakes3.ErrorMessagef(gofakes3.ErrNotImplemented, "part %d has already been streamed to the backend and cannot be replaced with different contents", partNumber)
			}
			return nil
		}
	}
	up.partMD5s[partNumber] = md5Sum
	up.partSizes[partNumber] = size
	up.streamBuf[partNumber] = rw

	if up.pumping {
		// Another goroutine owns the pipe and will pump this part in turn.
		up.mu.Unlock()
		return nil
	}
	up.pumping = true

	for {
		prw, ok := up.streamBuf[up.nextPart]
		if !ok {
			up.pumping = false
			up.mu.Unlock()
			return nil
		}
		delete(up.streamBuf, up.nextPart)
		psize := prw.Size()
		up.mu.Unlock()

		err := pipePart(up.sink(), prw)
		_ = prw.Close()
		if err != nil {
			up.mu.Lock()
			up.pumping = false
			up.buffered -= psize
			up.cond.Broadcast()
			up.mu.Unlock()
			return err
		}

		up.mu.Lock()
		up.nextPart++
		up.buffered -= psize
		up.cond.Broadcast()
	}
}

// pipePart writes the whole of rw into w (the pipe).
func pipePart(w io.Writer, rw *pool.RW) error {
	if _, err := rw.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := io.Copy(w, rw)
	return err
}

// CompleteMultipartUpload finalises a multipart upload. It closes the sink
// (committing a cached upload to the VFS cache, or finishing the background
// PutStream), moves the temporary object into place, computes the S3-style
// multipart ETag, and stores the user metadata so HeadObject and GetObject
// see the same fields the in-memory PutObject path produces.
func (b *s3Backend) CompleteMultipartUpload(ctx context.Context, bucketName, objectName string, uploadID gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (gofakes3.VersionID, string, error) {
	up, err := b.loadUpload(uploadID)
	if err != nil {
		return "", "", err
	}
	defer b.multipartUploads.Delete(uploadID)

	if err := up.validate(input); err != nil {
		_ = up.abort(ctx)
		b.discardUpload(ctx, up)
		return "", "", err
	}

	// All parts must have been streamed: contiguous part numbers from 1 with
	// nothing left buffered. A leftover means the client used non-contiguous
	// part numbers, which the in-order stream can't place.
	up.mu.Lock()
	streamed := up.nextPart - 1
	total := len(up.partSizes)
	leftover := len(up.streamBuf)
	up.mu.Unlock()
	if leftover != 0 || streamed != total {
		_ = up.abort(ctx)
		b.discardUpload(ctx, up)
		return "", "", gofakes3.ErrInvalidPart
	}

	if err := up.close(ctx); err != nil {
		b.discardUpload(ctx, up)
		return "", "", err
	}

	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return "", "", err
	}

	// Move the temporary object into place: a cached upload is renamed
	// through the VFS (its write-back then uploads it under the final name),
	// a streamed one is moved server-side on the backend.
	if up.streamFp != up.fp {
		if up.fh != nil {
			if err := _vfs.Rename(up.streamFp, up.fp); err != nil {
				b.discardUpload(ctx, up)
				return "", "", err
			}
		} else {
			if err := b.moveIntoPlace(ctx, _vfs.Fs(), up.streamFp, up.fp); err != nil {
				b.forgetPath(ctx, up.streamFp)
				b.forgetPath(ctx, up.fp)
				return "", "", err
			}
			b.forgetPath(ctx, up.streamFp)
		}
	}
	// The VFS is already consistent for a cached upload
	if up.fh == nil {
		b.forgetPath(ctx, up.fp)
	}

	b.meta.Store(up.fp, up.meta)
	if val, ok := up.meta["X-Amz-Meta-Mtime"]; ok {
		if ti, err := swift.FloatStringToTime(val); err == nil {
			b.storeModtime(up.fp, up.meta, val)
			_ = _vfs.Chtimes(up.fp, ti, ti)
		}
	} else if val, ok := up.meta["mtime"]; ok {
		if ti, err := swift.FloatStringToTime(val); err == nil {
			b.storeModtime(up.fp, up.meta, val)
			_ = _vfs.Chtimes(up.fp, ti, ti)
		}
	}

	return "", up.multipartETag(input), nil
}

// AbortMultipartUpload tears down an in-progress upload, discarding any data
// already received.
func (b *s3Backend) AbortMultipartUpload(ctx context.Context, bucketName, objectName string, uploadID gofakes3.UploadID) error {
	up, err := b.loadUpload(uploadID)
	if err != nil {
		return err
	}
	defer b.multipartUploads.Delete(uploadID)
	err = up.abort(ctx)
	b.discardUpload(ctx, up)
	return err
}

// discardUpload cleans up after a failed or aborted upload. It never touches
// the object at the final path: a cached upload only ever wrote to the
// temporary file in the VFS cache, which is removed (cancelling its
// write-back); a streamed upload's partial object is discarded by the failed
// PutStream (an atomic backend leaves the final object untouched, a
// non-atomic one only ever wrote the temporary object), so invalidating the
// VFS's view of streamFp is enough.
func (b *s3Backend) discardUpload(ctx context.Context, up *multipartUpload) {
	if up.fh != nil {
		if _vfs, err := b.s.getVFS(ctx); err == nil {
			_ = _vfs.Remove(up.streamFp)
		}
		return
	}
	b.forgetPath(ctx, up.streamFp)
}

// moveIntoPlace moves the temporary object srcFp to its final path dstFp on f,
// server-side, overwriting any object already there.
func (b *s3Backend) moveIntoPlace(ctx context.Context, f fs.Fs, srcFp, dstFp string) error {
	srcObj, err := f.NewObject(ctx, srcFp)
	if err != nil {
		return fmt.Errorf("failed to find uploaded object: %w", err)
	}
	if _, err := operations.Move(ctx, f, nil, dstFp, srcObj); err != nil {
		return fmt.Errorf("failed to move uploaded object into place: %w", err)
	}
	return nil
}

// forgetPath invalidates the parent directory's cached VFS listing so that
// subsequent VFS Stat / List calls re-read fp from the underlying Fs.
func (b *s3Backend) forgetPath(ctx context.Context, fp string) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return
	}
	if root, err := _vfs.Root(); err == nil {
		root.ForgetPath(fp, fs.EntryObject)
	}
}

// validate cross-checks the part list supplied by the client against the
// parts we actually received.
func (up *multipartUpload) validate(input *gofakes3.CompleteMultipartUploadRequest) error {
	up.mu.Lock()
	defer up.mu.Unlock()

	for i := 1; i < len(input.Parts); i++ {
		if input.Parts[i].PartNumber <= input.Parts[i-1].PartNumber {
			return gofakes3.ErrInvalidPartOrder
		}
	}
	if len(input.Parts) != len(up.partSizes) {
		return gofakes3.ErrInvalidPart
	}
	for _, p := range input.Parts {
		md5Sum, ok := up.partMD5s[p.PartNumber]
		if !ok {
			return gofakes3.ErrInvalidPart
		}
		clientETag := strings.Trim(p.ETag, `"`)
		if clientETag != hex.EncodeToString(md5Sum) {
			return gofakes3.ErrInvalidPart
		}
	}
	return nil
}

// close finalises the upload: a cached upload is committed to the VFS cache,
// a streamed one has EOF signalled to the background PutStream which is then
// waited for.
func (up *multipartUpload) close(ctx context.Context) error {
	up.mu.Lock()
	if up.closed {
		up.mu.Unlock()
		return nil
	}
	up.closed = true
	up.cond.Broadcast()
	up.mu.Unlock()

	if up.fh != nil {
		return up.fh.Close()
	}
	err := up.pipeW.Close()
	<-up.putDone
	up.putCancel()
	if up.putErr != nil {
		return up.putErr
	}
	return err
}

// errMultipartAborted makes the background PutStream fail when an upload is
// aborted, so it tears down its partial object instead of completing.
var errMultipartAborted = errors.New("serve s3: multipart upload aborted")

// abort tears down the upload and releases any buffered parts. A cached
// upload only ever wrote to the temporary file in the VFS cache, which the
// caller removes; a streamed one has its background PutStream failed so it
// discards its partial object.
func (up *multipartUpload) abort(ctx context.Context) error {
	up.mu.Lock()
	if up.closed {
		up.mu.Unlock()
		return nil
	}
	up.closed = true
	streamBuf := up.streamBuf
	up.streamBuf = nil
	up.cond.Broadcast()
	up.mu.Unlock()

	for _, rw := range streamBuf {
		_ = rw.Close()
	}

	if up.fh != nil {
		return up.fh.Close()
	}

	// Fail the background PutStream (so it discards its partial object) and
	// wait for it to return.
	up.putCancel()
	_ = up.pipeW.CloseWithError(errMultipartAborted)
	<-up.putDone
	return nil
}

// multipartETag computes the S3 multipart ETag for the assembled object:
//
//	hex(md5(concat(part_md5s_in_order))) + "-" + N
func (up *multipartUpload) multipartETag(input *gofakes3.CompleteMultipartUploadRequest) string {
	partNumbers := make([]int, 0, len(input.Parts))
	for _, p := range input.Parts {
		partNumbers = append(partNumbers, p.PartNumber)
	}
	sort.Ints(partNumbers)

	up.mu.Lock()
	concat := make([]byte, 0, len(partNumbers)*md5.Size)
	for _, n := range partNumbers {
		concat = append(concat, up.partMD5s[n]...)
	}
	up.mu.Unlock()

	sum := md5.Sum(concat)
	return fmt.Sprintf("%q", fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(partNumbers)))
}
