package storage

import (
	"os"
	"path/filepath"
	"testing"

	"raftkv/internal/state"
)

func TestRecoveryTornTailAndCompaction(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	img := Image{Term: 3, Vote: "node2", Commit: 1,
		Entries: []Entry{{Index: 1, Term: 3,
			Command: state.Command{Op: "put", Key: "a", Value: "b"}}},
	}
	if err := w.Save(img); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "raft.wal"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 25, 1, 2, 3, 4, 9}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	w, recovered, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Term != 3 || recovered.Vote != "node2" || len(recovered.Entries) != 1 {
		t.Fatalf("bad recovery: %+v", recovered)
	}
	img.Snapshot = Snapshot{Index: 1, Term: 3, Values: map[string]string{"a": "b"}}
	img.Entries = nil
	if err := w.Compact(img); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, recovered, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if recovered.Snapshot.Values["a"] != "b" || len(recovered.Entries) != 0 {
		t.Fatalf("bad compacted recovery: %+v", recovered)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.bin")); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptionAndExclusiveOwnership(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, _, err := Open(dir); err == nil {
		other.Close()
		t.Fatal("opened locked directory")
	}
	if err := w.Save(Image{Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "raft.wal")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if other, _, err := Open(dir); err == nil {
		other.Close()
		t.Fatal("accepted corrupt checksum")
	}
}
