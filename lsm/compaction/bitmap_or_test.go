package compaction

import (
	"path/filepath"
	"testing"

	"lsm_tree/lsm/sstable"
	"lsm_tree/record"

	"github.com/RoaringBitmap/roaring"
)

func bitmapOf(ids ...uint32) *roaring.Bitmap {
	bm := roaring.New()
	for _, id := range ids {
		bm.Add(id)
	}
	return bm
}

func TestMergeToBuilderBitmapOR_SimpleUnion(t *testing.T) {
	dir := t.TempDir()

	// --- создаём 2 SST с одним и тем же ключом ---
	makeTable := func(name string, key string, ids ...uint32) *sstable.SSTable {
		path := filepath.Join(dir, name)
		b, err := sstable.NewBuilder(path, 4, 10, 0.01)
		if err != nil {
			t.Fatal(err)
		}

		bm := bitmapOf(ids...)
		data, _ := bm.MarshalBinary()

		if err := b.Add(record.Entry{
			Key:   key,
			Value: data,
		}); err != nil {
			t.Fatal(err)
		}

		if err := b.Finish(); err != nil {
			t.Fatal(err)
		}

		tbl, err := sstable.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}

	t1 := makeTable("t1.sst", "apple", 1, 2)
	t2 := makeTable("t2.sst", "apple", 3)
	defer t1.Close()
	defer t2.Close()

	outPath := filepath.Join(dir, "out.sst")
	builder, err := sstable.NewBuilder(outPath, 4, 10, 0.01)
	if err != nil {
		t.Fatal(err)
	}

	err = MergeToBuilderBitmapOR([]*sstable.SSTable{t1, t2}, builder)
	if err != nil {
		t.Fatal(err)
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	outTable, err := sstable.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}

	e, found, err := outTable.Get("apple")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("apple not found in merged table")
	}

	res := roaring.New()
	if err := res.UnmarshalBinary(e.Value); err != nil {
		t.Fatal(err)
	}

	want := bitmapOf(1, 2, 3)
	if !res.Equals(want) {
		t.Fatalf("expected {1,2,3}, got %v", res.ToArray())
	}
	err = outTable.Close()
	if err != nil {
		return
	}
}

func TestMergeToBuilderBitmapOR_MultipleKeys(t *testing.T) {
	dir := t.TempDir()

	makeTable := func(name string, entries map[string][]uint32) *sstable.SSTable {
		path := filepath.Join(dir, name)
		b, err := sstable.NewBuilder(path, 4, 10, 0.01)
		if err != nil {
			t.Fatal(err)
		}

		// ВАЖНО: фиксированный порядок
		keys := []string{"apple", "banana"}
		for _, k := range keys {
			ids := entries[k]
			bm := bitmapOf(ids...)
			data, _ := bm.MarshalBinary()
			if err := b.Add(record.Entry{
				Key:   k,
				Value: data,
			}); err != nil {
				t.Fatal(err)
			}
		}

		if err := b.Finish(); err != nil {
			t.Fatal(err)
		}

		tbl, err := sstable.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}

	t1 := makeTable("t1.sst", map[string][]uint32{
		"apple":  {1},
		"banana": {2},
	})

	t2 := makeTable("t2.sst", map[string][]uint32{
		"apple":  {3},
		"banana": {4},
	})
	defer t1.Close()
	defer t2.Close()

	outPath := filepath.Join(dir, "out.sst")
	builder, _ := sstable.NewBuilder(outPath, 4, 10, 0.01)

	if err := MergeToBuilderBitmapOR([]*sstable.SSTable{t1, t2}, builder); err != nil {
		t.Fatal(err)
	}
	builder.Finish()

	out, _ := sstable.Open(outPath)
	defer out.Close()

	check := func(key string, expected ...uint32) {
		e, found, _ := out.Get(key)
		if !found {
			t.Fatalf("%s not found", key)
		}
		res := roaring.New()
		res.UnmarshalBinary(e.Value)
		want := bitmapOf(expected...)
		if !res.Equals(want) {
			t.Fatalf("%s expected %v got %v", key, expected, res.ToArray())
		}
	}

	check("apple", 1, 3)
	check("banana", 2, 4)
}
