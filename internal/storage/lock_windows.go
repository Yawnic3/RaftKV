//go:build windows

package storage

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	var overlapped syscall.Overlapped
	result, _, callErr := proc.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if result == 0 {
		return nil, errors.Join(callErr, f.Close())
	}
	return f, nil
}
