//go:build !linux

package osutil

// RSS returns the current process resident set size in bytes (unavailable on this platform).
func RSS() int64 { return 0 }
