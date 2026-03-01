package index

import (
	"os"
	"testing"

	"lsm_tree/lsm"

	"github.com/RoaringBitmap/roaring"
)

// setupTestLSM создаёт временное LSM для тестов с уменьшенными параметрами.
func setupTestLSM(t *testing.T) (*lsm.LSM, func()) {
	dir, err := os.MkdirTemp("", "lsm_test")
	if err != nil {
		t.Fatal(err)
	}

	opts := lsm.DefaultOptions(dir)
	opts.MemTableSize = 2 // маленький размер → гарантированный flush
	opts.BlockEntries = 5
	opts.MaxL0 = 2 // маленький порог → гарантированный compaction
	opts.CompactionMode = lsm.CompactionBitmapOR

	db, err := lsm.NewLSM(opts)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, cleanup
}

// roaringOf создаёт битмап из слайса uint32.
func roaringOf(ids ...uint32) *roaring.Bitmap {
	bm := roaring.New()
	for _, id := range ids {
		bm.Add(id)
	}
	return bm
}

func TestNormalizeWord(t *testing.T) {
	tests := []struct {
		word     string
		lang     string
		expected string
		keep     bool
	}{
		{"running", "en", "run", true},
		{"ran", "en", "ran", true},
		{"the", "en", "", false},
		{"dogs", "en", "dog", true},
		{"бежать", "ru", "бежа", true},
		{"бег", "ru", "бег", true},
		{"и", "ru", "", false},
		{"лиса", "ru", "лис", true},
		{"собака", "ru", "собак", true},
		{"", "en", "", false},
		{"123", "en", "123", true},
		{"!@#", "en", "", false},
	}

	for _, tt := range tests {
		got, ok := normalizeWord(tt.word, tt.lang)
		if ok != tt.keep {
			t.Errorf("normalizeWord(%q, %q) keep = %v, want %v", tt.word, tt.lang, ok, tt.keep)
		}
		if got != tt.expected {
			t.Errorf("normalizeWord(%q, %q) = %q, want %q", tt.word, tt.lang, got, tt.expected)
		}
	}
}

func TestTokenize(t *testing.T) {
	text := "Hello, world! This is a test."
	expected := []string{"Hello", "world", "This", "is", "a", "test"}
	got := tokenize(text)
	if len(got) != len(expected) {
		t.Fatalf("len(got)=%d, want %d", len(got), len(expected))
	}
	for i := range got {
		if got[i] != expected[i] {
			t.Errorf("got[%d]=%q, want %q", i, got[i], expected[i])
		}
	}
}

func TestIndexer_English(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db) // язык en

	docs := []struct {
		id   uint32
		text string
	}{
		{1, "The quick brown fox jumps over the lazy dog"},
		{2, "A quick brown dog outpaces a quick fox"},
		{3, "Lazy dogs sleep all day"},
		{4, "Foxes are quick and clever"},
	}

	for _, d := range docs {
		if err := idx.IndexDocument(d.id, d.text); err != nil {
			t.Fatalf("IndexDocument(%d) error: %v", d.id, err)
		}
	}

	tests := []struct {
		query    string
		expected []uint32
	}{
		{"quick", []uint32{1, 2, 4}},
		{"fox", []uint32{1, 2, 4}}, // foxes -> fox
		{"dog", []uint32{1, 2, 3}}, // dogs -> dog, добавляется документ 3
		{"lazy", []uint32{1, 3}},
		{"quick AND fox", []uint32{1, 2, 4}},
		{"quick OR lazy", []uint32{1, 2, 3, 4}},
		{"quick AND NOT dog", []uint32{4}}, // quick {1,2,4} минус dog {1,2,3} = {4}
		{"fox AND dog", []uint32{1, 2}},    // fox {1,2,4} пересечение dog {1,2,3} = {1,2}
		{"the", []uint32{}},                // стоп-слово
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			results, err := idx.Search(tt.query)
			if err != nil {
				t.Fatalf("Search(%q) error: %v", tt.query, err)
			}
			got := roaringOf(results...)
			want := roaringOf(tt.expected...)
			if !got.Equals(want) {
				t.Errorf("Search(%q) = %v, want %v", tt.query, results, tt.expected)
			}
		})
	}
}

func TestIndexer_Russian(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexerWithLang(db, "ru")

	docs := []struct {
		id   uint32
		text string
	}{
		{1, "Быстрая коричневая лиса прыгает через ленивую собаку"},
		{2, "Быстрая коричневая собака обгоняет быструю лису"},
		{3, "Ленивые собаки спят весь день"},
		{4, "Лисы быстры и хитры"},
	}

	for _, d := range docs {
		if err := idx.IndexDocument(d.id, d.text); err != nil {
			t.Fatalf("IndexDocument(%d) error: %v", d.id, err)
		}
	}

	// Операторы используем английские (AND, OR, NOT)
	tests := []struct {
		query    string
		expected []uint32
	}{
		{"быстрый", []uint32{1, 2, 4}}, // быстр
		{"лиса", []uint32{1, 2, 4}},    // лис
		{"собака", []uint32{1, 2, 3}},  // собак (документ 3 тоже)
		{"ленивый", []uint32{1, 3}},    // ленив
		{"быстрый AND лиса", []uint32{1, 2, 4}},
		{"быстрый OR ленивый", []uint32{1, 2, 3, 4}},
		{"быстрый AND NOT собака", []uint32{4}}, // быстр {1,2,4} минус собак {1,2,3} = {4}
		{"лиса AND собака", []uint32{1, 2}},     // лис {1,2,4} пересеч собак {1,2,3} = {1,2}
		{"и", []uint32{}},                       // стоп-слово
		{"день", []uint32{3}},                   // день -> ден (в индексе)
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			results, err := idx.Search(tt.query)
			if err != nil {
				t.Fatalf("Search(%q) error: %v", tt.query, err)
			}
			got := roaringOf(results...)
			want := roaringOf(tt.expected...)
			if !got.Equals(want) {
				t.Errorf("Search(%q) = %v, want %v", tt.query, results, tt.expected)
			}
		})
	}
}

func TestIndexer_EmptyIndex(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	results, err := idx.Search("quick")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %v", results)
	}
}

func TestIndexer_InvalidQuery(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "test") // чтобы индекс не был пустым

	// Пустой запрос должен возвращать ошибку (невалидный синтаксис)
	_, err := idx.Search("")
	if err == nil {
		t.Error("expected error for empty query, got nil")
	}

	_, err = idx.Search("quick AND") // незаконченный оператор
	if err == nil {
		t.Error("expected error for invalid query, got nil")
	}

	_, err = idx.Search("(quick fox") // незакрытая скобка
	if err == nil {
		t.Error("expected error for mismatched parentheses, got nil")
	}
}
