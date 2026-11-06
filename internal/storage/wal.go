package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const maxRecord = 256 << 20

type WAL struct {
	dir  string
	file *os.File
	lock *os.File
}

// Open acquires an OS-backed exclusive lock on dir before reading the WAL.
func Open(dir string) (*WAL, Image, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, Image{}, err
	}
	lock, err := lockFile(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, Image{}, fmt.Errorf("lock data directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "raft.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, Image{}, errors.Join(err, lock.Close())
	}
	w := &WAL{dir: dir, file: f, lock: lock}
	img, err := w.recover()
	if err == nil {
		err = syncDirectory(dir)
	}
	if err != nil {
		return nil, Image{}, errors.Join(err, f.Close(), lock.Close())
	}
	return w, img, nil
}

func (w *WAL) recover() (Image, error) {
	var img Image
	var offset int64
	for {
		var header [8]byte
		_, err := io.ReadFull(w.file, header[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := w.file.Truncate(offset); err != nil {
				return img, err
			}
			break
		}
		if err != nil {
			return img, err
		}
		size := binary.BigEndian.Uint32(header[:4])
		if size == 0 || size > maxRecord {
			return img, fmt.Errorf("invalid WAL length at %d", offset)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if err := w.file.Truncate(offset); err != nil {
					return img, err
				}
				break
			}
			return img, err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return img, fmt.Errorf("WAL checksum mismatch at %d", offset)
		}
		if err := json.Unmarshal(payload, &img); err != nil {
			return img, fmt.Errorf("decode WAL: %w", err)
		}
		offset += int64(8 + size)
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return img, err
}

func frame(img Image) ([]byte, error) {
	payload, err := json.Marshal(img)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRecord {
		return nil, fmt.Errorf("WAL image exceeds %d bytes", maxRecord)
	}
	buf := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	return buf, nil
}

func writeSync(f *os.File, data []byte) error {
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return f.Sync()
}

func (w *WAL) Save(img Image) error {
	buf, err := frame(img)
	if err != nil {
		return err
	}
	return writeSync(w.file, buf)
}

// Compact first publishes a self-contained disk snapshot, then atomically
// replaces the WAL with the same recovery image. Either WAL generation is safe.
func (w *WAL) Compact(img Image) error {
	buf, err := frame(img)
	if err != nil {
		return err
	}
	if err := atomicWrite(w.dir, "snapshot.bin", buf); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := atomicWrite(w.dir, "raft.wal", buf); err != nil {
		return err
	}
	w.file, err = os.OpenFile(filepath.Join(w.dir, "raft.wal"), os.O_RDWR|os.O_APPEND, 0600)
	return err
}

func atomicWrite(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = writeSync(f, data)
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (w *WAL) Close() error { return errors.Join(w.file.Close(), w.lock.Close()) }
