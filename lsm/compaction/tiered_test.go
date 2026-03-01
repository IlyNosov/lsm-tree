package compaction

import (
	"os"
	"path/filepath"
	"testing"

	"lsm_tree/lsm/sstable"
	"lsm_tree/record"
)

func createTable(t *testing.T, dir string, name string, entries []record.Entry) *sstable.SSTable {
	path := filepath.Join(dir, name)

	b, err := sstable.NewBuilder(path, 4, uint64(len(entries)), 0.01)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		if err := b.Add(e); err != nil {
			t.Fatal(err)
		}
	}

	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	table, err := sstable.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	return table
}

func TestMergeToBuilder(t *testing.T) {
	dir := "./testdata"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// table 1 (старее)
	t1 := createTable(t, dir, "t1.sst", []record.Entry{
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
		{Key: "c", Value: []byte("3"), Seq: 3},
	})

	// table 2 (новее)
	t2 := createTable(t, dir, "t2.sst", []record.Entry{
		{Key: "b", Value: []byte("20"), Seq: 5}, // overwrite
		{Key: "d", Value: []byte("4"), Seq: 6},
	})

	outPath := filepath.Join(dir, "merged.sst")
	b, err := sstable.NewBuilder(outPath, 4, 10, 0.01)
	if err != nil {
		t.Fatal(err)
	}

	err = MergeToBuilder([]*sstable.SSTable{t1, t2}, b)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	merged, err := sstable.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}

	// проверяем значения
	check := func(key string, expected string) {
		e, found, err := merged.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key %s not found", key)
		}
		if string(e.Value) != expected {
			t.Fatalf("key %s expected %s got %s", key, expected, string(e.Value))
		}
	}

	check("a", "1")
	check("b", "20") // должно взять более новый
	check("c", "3")
	check("d", "4")
}
