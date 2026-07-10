package pkg

import (
	"github.com/oarkflow/bcl"
)

// unmarshalBCLRaw is the single import point for the BCL library. All other
// files in this package remain dependency-free.
func unmarshalBCLRaw(src []byte, v any) error {
	return bcl.Unmarshal(src, v)
}

// marshalBCL serializes a Go value to BCL format.
func marshalBCL(v any) ([]byte, error) {
	return bcl.Marshal(v)
}

// unmarshalBCLOptions unmarshals BCL with custom options.
func unmarshalBCLOptions(src []byte, v any, opts *bcl.Options) error {
	return bcl.UnmarshalWithOptions(src, v, opts)
}
