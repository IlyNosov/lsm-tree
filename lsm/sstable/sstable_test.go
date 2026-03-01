package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"lsm_tree/record"
)

func TestSSTableBuildAndGet(t *testing.T) {

	dir := "./testdata"
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0755)

	path := filepath.Join(dir, "test.sst")

	entries := []record.Entry{
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
		{Key: "c", Value: []byte("3"), Seq: 3},
	}

	b, err := NewBuilder(path, 2, uint64(len(entries)), 0.01)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Build(entries); err != nil {
		t.Fatal(err)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	e, found, err := table.Get("b")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected to find key b")
	}
	if string(e.Value) != "2" {
		t.Fatal("wrong value")
	}
}

func TestSSTableMultipleBlocks(t *testing.T) {
	dir := "./testdata2"
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0755)

	path := filepath.Join(dir, "multi.sst")

	entries := []record.Entry{
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
		{Key: "c", Value: []byte("3"), Seq: 3},
	}

	b, err := NewBuilder(path, 1, 3, 0.01)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Build(entries); err != nil {
		t.Fatal(err)
	}

	table, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	e, found, err := table.Get("c")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(e.Value) != "3" {
		t.Fatal("expected to find c=3")
	}
}
