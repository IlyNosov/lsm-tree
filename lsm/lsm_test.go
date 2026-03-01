package lsm

import (
	"os"
	"testing"
)

func TestLSMPutGet(t *testing.T) {

	dir := "./testdb"
	_ = os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2
	opts.BlockEntries = 2

	db, err := NewLSM(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}

	// flush должен произойти
	v, ok, err := db.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(v) != "1" {
		t.Fatal("wrong value for a")
	}
}

func TestLSMDelete(t *testing.T) {

	dir := "./testdb2"
	_ = os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2
	opts.BlockEntries = 2

	db, err := NewLSM(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put("a", []byte("1"))
	db.Put("b", []byte("2")) // flush

	db.Delete("a")
	db.Put("c", []byte("3")) // flush

	_, ok, err := db.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected a to be deleted")
	}
}

func TestLSMCompaction(t *testing.T) {
	dir := "./testcompact"
	_ = os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1
	opts.MaxL0 = 2
	opts.BlockEntries = 1

	db, err := NewLSM(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put("a", []byte("1"))
	db.Put("b", []byte("2"))
	db.Put("c", []byte("3"))

	files, _ := os.ReadDir(dir)
	if len(files) > 2 {
		t.Fatal("expected compaction to reduce files")
	}
}
