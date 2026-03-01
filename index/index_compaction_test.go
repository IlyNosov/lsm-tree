package index

import (
	"os"
	"path/filepath"
	"testing"

	"lsm_tree/lsm"
)

func TestBitmapUnionHappensOnCompaction(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "invlsm_test")
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	opts.Dir = dir

	// ВАЖНО: маленькие пороги, чтобы гарантировать flush/compaction
	opts.MemTableSize = 1                        // после каждого Put будет flush
	opts.MaxL0 = 2                               // как только >=2 таблицы — компактим (если у тебя так устроено)
	opts.CompactionMode = lsm.CompactionBitmapOR // твой новый режим

	engine, err := lsm.NewLSM(opts)
	if err != nil {
		t.Fatal(err)
	}

	idx := NewIndexerWithLang(engine, "en")

	// Документ 1
	if err := idx.IndexDocument(1, "hello world"); err != nil {
		t.Fatal(err)
	}
	// Документ 2 (тот же термин hello)
	if err := idx.IndexDocument(2, "hello again"); err != nil {
		t.Fatal(err)
	}

	// Ищем hello
	got, err := idx.Search("hello")
	if err != nil {
		t.Fatal(err)
	}

	// Проверяем, что оба docID есть
	want := map[uint32]bool{1: true, 2: true}
	for _, id := range got {
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("missing docIDs after compaction: %+v, got=%v", want, got)
	}
}

func TestIndexer_FlushUnionWithoutCompaction(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// Два документа с одинаковым словом
	if err := idx.IndexDocument(1, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexDocument(2, "hello"); err != nil {
		t.Fatal(err)
	}

	results, err := idx.Search("hello")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	want := roaringOf(1, 2)

	if !got.Equals(want) {
		t.Fatalf("expected {1,2}, got %v", results)
	}
}

func TestIndexer_CompactionBitmapOR(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// Создаём много документов, чтобы точно было несколько flush
	for i := uint32(1); i <= 5; i++ {
		if err := idx.IndexDocument(i, "compaction"); err != nil {
			t.Fatal(err)
		}
	}

	// Теперь ищем
	results, err := idx.Search("compaction")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	want := roaringOf(1, 2, 3, 4, 5)

	if !got.Equals(want) {
		t.Fatalf("expected {1..5}, got %v", results)
	}
}

func TestIndexer_NOT_AfterCompaction(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	if err := idx.IndexDocument(1, "alpha beta"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexDocument(2, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexDocument(3, "beta"); err != nil {
		t.Fatal(err)
	}

	results, err := idx.Search("alpha AND NOT beta")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	want := roaringOf(2)

	if !got.Equals(want) {
		t.Fatalf("expected {2}, got %v", results)
	}
}

func TestIndexer_MultipleTermsCompaction(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	if err := idx.IndexDocument(1, "apple banana"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexDocument(2, "apple"); err != nil {
		t.Fatal(err)
	}
	if err := idx.IndexDocument(3, "banana"); err != nil {
		t.Fatal(err)
	}

	r1, _ := idx.Search("apple")
	r2, _ := idx.Search("banana")

	if !roaringOf(r1...).Equals(roaringOf(1, 2)) {
		t.Fatalf("apple incorrect: %v", r1)
	}
	if !roaringOf(r2...).Equals(roaringOf(1, 3)) {
		t.Fatalf("banana incorrect: %v", r2)
	}
}
