// Package qualification provides an explicitly volatile test block device.
// It is not part of tarp's storage implementation or public API.
package qualification

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	pageBytes = 4096
	MaxDisk   = 256 << 20
)

type Stats struct {
	Writes         uint64 `json:"writes"`
	WrittenBytes   uint64 `json:"written_bytes"`
	Flushes        uint64 `json:"flushes"`
	FUAWrites      uint64 `json:"fua_writes"`
	PersistedBytes uint64 `json:"persisted_bytes"`
	DirtyBytes     uint64 `json:"dirty_bytes"`
	DiscardedBytes uint64 `json:"discarded_bytes"`
}

// Disk acknowledges ordinary writes into RAM only. Flush and FUA complete only
// after the backing file is synced. Close deliberately does not flush.
type Disk struct {
	mu    sync.Mutex
	file  *os.File
	size  int64
	dirty map[int64][]byte
	stats Stats
}

func CreateDisk(path string, size int64) (*Disk, error) {
	if size <= 0 || size > MaxDisk || size%pageBytes != 0 {
		return nil, fmt.Errorf("qualification: disk size must be page-aligned and at most %d", MaxDisk)
	}
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = fd.Truncate(size); err == nil {
		err = fd.Sync()
	}
	if err == nil {
		var parent *os.File
		parent, err = os.Open(filepath.Dir(path))
		if err == nil {
			err = errors.Join(parent.Sync(), parent.Close())
		}
	}
	if err != nil {
		return nil, errors.Join(err, fd.Close())
	}
	return &Disk{file: fd, size: size, dirty: make(map[int64][]byte)}, nil
}

func (d *Disk) Size() int64 { return d.size }

func (d *Disk) bounds(offset int64, length int) error {
	if offset < 0 || offset > d.size || int64(length) > d.size-offset {
		return fmt.Errorf("qualification: request outside disk: offset=%d length=%d", offset, length)
	}
	return nil
}

func (d *Disk) ReadAt(data []byte, offset int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.bounds(offset, len(data)); err != nil {
		return err
	}
	for len(data) != 0 {
		page := offset / pageBytes * pageBytes
		within := int(offset - page)
		n := min(len(data), pageBytes-within)
		if cached, ok := d.dirty[page]; ok {
			copy(data[:n], cached[within:within+n])
		} else if _, err := d.file.ReadAt(data[:n], offset); err != nil {
			return err
		}
		data, offset = data[n:], offset+int64(n)
	}
	return nil
}

func (d *Disk) WriteAt(data []byte, offset int64, fua bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.bounds(offset, len(data)); err != nil {
		return err
	}
	length := len(data)
	for len(data) != 0 {
		page := offset / pageBytes * pageBytes
		cached, ok := d.dirty[page]
		if !ok {
			cached = make([]byte, pageBytes)
			if _, err := d.file.ReadAt(cached, page); err != nil {
				return err
			}
			d.dirty[page] = cached
		}
		within := int(offset - page)
		n := min(len(data), pageBytes-within)
		copy(cached[within:within+n], data[:n])
		data, offset = data[n:], offset+int64(n)
	}
	d.stats.Writes++
	d.stats.WrittenBytes += uint64(length)
	if fua {
		if err := d.flush(); err != nil {
			return err
		}
		d.stats.FUAWrites++
	}
	return nil
}

func (d *Disk) flush() error {
	pages := make([]int64, 0, len(d.dirty))
	for page := range d.dirty {
		pages = append(pages, page)
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i] < pages[j] })
	for _, page := range pages {
		n, err := d.file.WriteAt(d.dirty[page], page)
		if err != nil {
			return err
		}
		if n != pageBytes {
			return io.ErrShortWrite
		}
	}
	if err := d.file.Sync(); err != nil {
		return err
	}
	d.stats.PersistedBytes += uint64(len(pages) * pageBytes)
	clear(d.dirty)
	d.stats.Flushes++
	return nil
}

func (d *Disk) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.flush()
}

// PowerCut must follow termination of the client. It discards, rather than
// writing back, every cached page, including previously acknowledged writes.
func (d *Disk) PowerCut() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.DiscardedBytes += uint64(len(d.dirty) * pageBytes)
	clear(d.dirty)
	return d.snapshot()
}

func (d *Disk) snapshot() Stats {
	result := d.stats
	result.DirtyBytes = uint64(len(d.dirty) * pageBytes)
	return result
}

func (d *Disk) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.snapshot()
}

func (d *Disk) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
