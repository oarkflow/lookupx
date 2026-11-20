//go:build unix

package pkg

import (
	"golang.org/x/sys/unix"
	"os"
)

const segmentMappingUsesOS = true

func mapSegmentFile(path string) ([]byte, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if info.Size() <= 0 || uint64(info.Size()) > uint64(^uint(0)>>1) {
		return nil, nil, os.ErrInvalid
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	_ = unix.Madvise(data, unix.MADV_RANDOM)
	return data, func() error { return unix.Munmap(data) }, nil
}
