//go:build !linux

package pkg

// CPU process accounting is currently implemented on Linux. Returning zero on
// other platforms is preferable to reporting runtime CPU capacity as used CPU.
func processCPUSeconds() float64 { return 0 }
