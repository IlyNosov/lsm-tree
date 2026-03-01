package lsm_test

import (
	"fmt"
	"os"
	"testing"

	"lsm_tree/lsm"
)

func setupDB(b *testing.B) *lsm.LSM {
	dir := "./benchdata"
	_ = os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	opts.MemTableSize = 1024
	opts.BlockEntries = 64

	db, err := lsm.NewLSM(opts)
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkPut(b *testing.B) {
	db := setupDB(b)
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := db.Put(key, []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetHit(b *testing.B) {
	db := setupDB(b)
	defer db.Close()

	for i := 0; i < 10000; i++ {
		db.Put(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		_, _, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMiss(b *testing.B) {
	db := setupDB(b)
	defer db.Close()

	for i := 0; i < 10000; i++ {
		db.Put(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("not-exist-%d", i)
		_, _, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRangeShort(b *testing.B) {
	db := setupDB(b)
	defer db.Close()

	for i := 0; i < 10000; i++ {
		db.Put(fmt.Sprintf("key-%05d", i), []byte("value"))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Range("key-01000", "key-01020")
		if err != nil {
			b.Fatal(err)
		}
	}
}
