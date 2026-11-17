package pkg

import (
	"runtime"
)

type ResourceUsage struct {
	HeapBytes       uint64  `json:"heap_bytes"`
	HeapInUseBytes  uint64  `json:"heap_in_use_bytes"`
	SystemBytes     uint64  `json:"system_bytes"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	Mallocs         uint64  `json:"mallocs"`
	Goroutines      int     `json:"goroutines"`
	CPUSeconds      float64 `json:"cpu_seconds"`
}

func readResourceUsage() ResourceUsage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return ResourceUsage{HeapBytes: m.HeapAlloc, HeapInUseBytes: m.HeapInuse, SystemBytes: m.Sys, TotalAllocBytes: m.TotalAlloc, Mallocs: m.Mallocs, Goroutines: runtime.NumGoroutine(), CPUSeconds: processCPUSeconds()}
}

func usageDelta(before, after ResourceUsage) ResourceUsage {
	return ResourceUsage{HeapBytes: after.HeapBytes, HeapInUseBytes: after.HeapInUseBytes, SystemBytes: after.SystemBytes,
		TotalAllocBytes: after.TotalAllocBytes - before.TotalAllocBytes, Mallocs: after.Mallocs - before.Mallocs,
		Goroutines: after.Goroutines, CPUSeconds: after.CPUSeconds - before.CPUSeconds}
}
