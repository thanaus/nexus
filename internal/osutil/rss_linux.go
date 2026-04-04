//go:build linux

package osutil

import (
	"fmt"
	"os"
)

// RSS returns the current process resident set size in bytes.
func RSS() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var dummy, rss int64
	_, _ = fmt.Sscanf(string(data), "%d %d", &dummy, &rss)
	return rss * int64(os.Getpagesize())
}
