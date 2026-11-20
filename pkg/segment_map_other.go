//go:build !unix

package pkg

import "os"

const segmentMappingUsesOS = false

func mapSegmentFile(path string) ([]byte, func() error, error) {
	data, err := os.ReadFile(path)
	return data, func() error { return nil }, err
}
