//go:build linux

package block

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Memfd wraps a memfd received from Firecracker
type Memfd struct {
	fd   int
	size int

	mmapOnce sync.Once
	mmap     []byte
	mmapErr  error
}

// NewFromFd creates a new Memfd wrapper of a memfd object (fd) that
// backs memory of size bytes big
func NewFromFd(fd, size int) *Memfd {
	return &Memfd{
		fd:   fd,
		size: size,
	}
}

// ensureMapped lazily mmaps the whole memfd. Safe to call from multiple
// goroutines; the mapping is performed exactly once.
func (m *Memfd) ensureMapped() error {
	m.mmapOnce.Do(func() {
		mm, err := syscall.Mmap(m.fd, 0, m.size, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			m.mmapErr = fmt.Errorf("failed to mmap memfd: %w", err)

			return
		}
		m.mmap = mm
	})

	return m.mmapErr
}

// Slice returns a zero-copy view of [offset, offset+size) of the memfd.
// The returned slice is valid until Close is called.
func (m *Memfd) Slice(offset, size int64) ([]byte, error) {
	if err := m.ensureMapped(); err != nil {
		return nil, err
	}
	if offset < 0 || offset >= int64(m.size) || offset+size > int64(m.size) {
		return nil, fmt.Errorf("range [%d, %d) out of bounds (size %d)", offset, offset+size, m.size)
	}

	return m.mmap[offset : offset+size], nil
}

// Close unmaps memory if it was previously mmap'ed and closes the memfd file descriptor
// if not already closed.
func (m *Memfd) Close() error {
	var err error

	if m.mmap != nil {
		if e := syscall.Munmap(m.mmap); e != nil {
			err = fmt.Errorf("munmap memfd: %w", e)
		}
		m.mmap = nil
	}

	if m.fd >= 0 {
		if e := syscall.Close(m.fd); e != nil {
			err = errors.Join(err, fmt.Errorf("close memfd: %w", e))
		}
		m.fd = -1
	}

	return err
}

// MemfdCache wraps a *Cache that is being populated from a memfd.
type MemfdCache struct {
	cache *Cache
	memfd *Memfd
}

func writeAll(fd int, off int64, buff []byte) error {
	remaining := len(buff)
	buffOff := 0

	for remaining > 0 {
		n, err := unix.Pwrite(fd, buff[buffOff:], off)
		if errors.Is(err, syscall.EINTR) {
			continue
		}

		if err != nil {
			return err
		}

		if n == 0 {
			return fmt.Errorf("pwrite: EOF with %d bytes remaining", remaining)
		}

		remaining -= n
		buffOff += n
		off += int64(n)
	}

	return nil
}

func dedupRange(
	ctx context.Context,
	originalMemfile ReadonlyDevice,
	f *os.File,
	off int64,
	blockSize int64,
	pageDirty, pageEmpty *roaring.Bitmap,
	memfd *Memfd,
	r *Range,
	buff []byte,
) (int64, error) {
	for chunkOff := int64(0); chunkOff < r.Size; chunkOff += blockSize {
		select {
		case <-ctx.Done():
			return off, ctx.Err()
		default:
		}

		srcBuf, err := memfd.Slice(r.Start+chunkOff, blockSize)
		if err != nil {
			return off, err
		}

		// build.File.ReadAt skips writes for uuid.Nil mappings and relies on
		// the caller to pre-zero the buffer; buff is reused across chunkOff
		// iterations (and across dedupRange calls — caller allocates once),
		// so without this clear we would compare srcBuf against stale bytes
		// for any region the parent serves as zero. Mirrors Cache.Dedup.
		clear(buff)
		_, err = originalMemfile.ReadAt(ctx, buff, r.Start+chunkOff)
		if err != nil {
			return off, fmt.Errorf("failed to read original memfile at offset %d: %w", r.Start+chunkOff, err)
		}

		for i := int64(0); i < blockSize; i += header.PageSize {
			srcPage := srcBuf[i : i+header.PageSize]
			pageIdx := uint32((r.Start + chunkOff + i) / header.PageSize)

			if bytes.Equal(srcPage, buff[i:i+header.PageSize]) {
				// Page matches base. If it's all-zero, mark it as Empty so
				// the merged header emits a uuid.Nil mapping for it — the
				// read path serves zero locally instead of falling through
				// to the parent's diff (which may be the synthetic Empty
				// template with no real backing). Non-zero matches stay
				// unmapped so the merge keeps the parent mapping.
				if header.IsZero(srcPage) {
					pageEmpty.Add(pageIdx)
				}

				continue
			}

			pageDirty.Add(pageIdx)
			if err = writeAll(int(f.Fd()), off, srcPage); err != nil {
				return off, err
			}

			off += header.PageSize
		}
	}

	return off, nil
}

func NewCacheFromMemfdDeduped(
	ctx context.Context,
	originalMemfile ReadonlyDevice,
	blockSize int64,
	filePath string,
	memfd *Memfd,
	ranges []Range,
) (*MemfdCache, *header.DiffMetadata, error) {
	ctx, span := tracer.Start(ctx, "new-cache-from-memfd-deduped")
	defer span.End()

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("error opening cache file: %w", err), memfd.Close())
	}

	var fileOff int64
	pageDirty := roaring.NewBitmap()
	pageEmpty := roaring.NewBitmap()
	buff := make([]byte, blockSize)
	var exportedSize int64
	for _, r := range ranges {
		exportedSize += r.Size
		fileOff, err = dedupRange(ctx, originalMemfile, f, fileOff, blockSize, pageDirty, pageEmpty, memfd, &r, buff)
		if err != nil {
			return nil, nil, errors.Join(err, f.Close(), memfd.Close(), os.Remove(filePath))
		}
	}

	if err = f.Close(); err != nil {
		return nil, nil, errors.Join(err, memfd.Close(), os.Remove(filePath))
	}

	cache, err := NewCache(fileOff, header.PageSize, filePath, false)
	if err != nil {
		return nil, nil, errors.Join(err, memfd.Close(), os.Remove(filePath))
	}
	cache.setIsCached(0, fileOff)

	// All source bytes are on disk in `cache`; the memfd is no longer needed.
	// Release the mmap (and on hugetlbfs the reservation) eagerly so we
	// don't pin host memory for the lifetime of the MemfdCache.
	if err = memfd.Close(); err != nil {
		logger.L().Warn(ctx, "Could not close memfd after dedup", zap.Error(err))
	}

	totalPages := exportedSize / header.PageSize
	uniquePages := int64(pageDirty.GetCardinality())
	dedupedPages := totalPages - uniquePages

	telemetry.SetAttributes(
		ctx,
		attribute.Int64("dedup.total_pages", totalPages),
		attribute.Int64("dedup.deduped_pages", dedupedPages),
		attribute.Int64("dedup.unique_pages", uniquePages),
		attribute.Float64("dedup.ratio", safeDivide(float64(dedupedPages), float64(totalPages))),
	)

	logger.L().Info(ctx, "4KiB page dedup completed (memfd fast-path)",
		zap.Int("ranges", len(ranges)),
		zap.Int64("total_4k_pages", totalPages),
		zap.Int64("deduped_pages", dedupedPages),
		zap.Int64("unique_pages", uniquePages),
		zap.Int64("exported_size_bytes", exportedSize),
		zap.Int64("dedup_size_bytes", fileOff),
		zap.String("reduction", fmt.Sprintf("%.1f%%", safeDivide(float64(dedupedPages), float64(totalPages))*100)),
	)

	return &MemfdCache{cache: cache}, &header.DiffMetadata{
		Dirty:     pageDirty,
		Empty:     pageEmpty,
		BlockSize: header.PageSize,
	}, nil
}

func safeDivide(a, b float64) float64 {
	if b == 0 {
		return 0
	}

	return a / b
}

func NewCacheFromMemfd(
	ctx context.Context,
	blockSize int64,
	filePath string,
	memfd *Memfd,
	ranges []Range,
) (*MemfdCache, error) {
	ctx, span := tracer.Start(ctx, "new-cache-from-memfd")
	defer span.End()

	size := GetSize(ranges)

	cache, err := NewCache(size, blockSize, filePath, false)
	if err != nil {
		if closeErr := memfd.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}

		return nil, err
	}

	if size == 0 {
		// We can close Memfd. We won't be reading anything out of it.
		if closeErr := memfd.Close(); closeErr != nil {
			return nil, errors.Join(fmt.Errorf("close memfd: %w", closeErr), cache.Close())
		}

		return &MemfdCache{cache: cache}, nil
	}

	memfdCache := &MemfdCache{
		cache: cache,
		memfd: memfd,
	}

	err = memfdCache.writeToDisk(ctx, ranges)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("could not write memfd to disk: %w", err), memfdCache.Close())
	}

	// Close memfd to release the memory
	// At the moment, we always close it. In the future, we will implement
	// copying at the background, so the file descriptor will be kept valid
	if err := memfdCache.memfd.Close(); err != nil {
		logger.L().Warn(ctx, "Could not close memfd", zap.Error(err))
	}
	memfdCache.memfd = nil

	return memfdCache, nil
}

func (m *MemfdCache) writeToDisk(ctx context.Context, ranges []Range) error {
	var cacheOff int64

	for _, r := range ranges {
		rangeStart := cacheOff

		src, err := m.memfd.Slice(r.Start, r.Size)
		if err != nil {
			return fmt.Errorf("bad memfd slice [%d,%d): %w", r.Start, r.Start+r.Size, err)
		}

		for srcOff := int64(0); srcOff < r.Size; srcOff += m.cache.blockSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n := min(m.cache.blockSize, r.Size-srcOff)
			copy((*m.cache.mmap)[cacheOff:cacheOff+n], src[srcOff:srcOff+n])
			cacheOff += n
		}

		m.cache.setIsCached(rangeStart, r.Size)
	}

	return nil
}

func (m *MemfdCache) ReadAt(b []byte, off int64) (int, error) {
	return m.cache.ReadAt(b, off)
}

func (m *MemfdCache) Slice(off, length int64) ([]byte, error) {
	return m.cache.Slice(off, length)
}

func (m *MemfdCache) Size() (int64, error) {
	return m.cache.Size()
}

func (m *MemfdCache) FileSize() (int64, error) {
	return m.cache.FileSize()
}

func (m *MemfdCache) BlockSize() int64 {
	return m.cache.BlockSize()
}

func (m *MemfdCache) Path() string {
	return m.cache.Path()
}

func (m *MemfdCache) Close() error {
	var err error

	if m.memfd != nil {
		if e := m.memfd.Close(); e != nil {
			err = fmt.Errorf("error closing memfd: %w", e)
		}
	}

	if e := m.cache.Close(); e != nil {
		err = errors.Join(err, fmt.Errorf("error closing cache: %w", e))
	}

	return err
}
