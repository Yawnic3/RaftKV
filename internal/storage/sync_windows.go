//go:build windows

package storage

// Windows does not expose directory fsync through os.File. File contents are
// flushed, but power-loss durability of rename is outside this platform's claim.
func syncDirectory(string) error { return nil }
