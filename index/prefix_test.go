package index

import (
	"testing"
)

func TestSearchPrefix_Basic(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// индексируем документы с разными словами
	docs := []struct {
		id   uint32
		text string
	}{
		{1, "running fast"},
		{2, "runner wins race"},
		{3, "run every morning"},
		{4, "sleeping well"},
	}

	for _, d := range docs {
		if err := idx.IndexDocument(d.id, d.text); err != nil {
			t.Fatal(err)
		}
	}

	// после стемминга: running->run, runner->runner, run->run
	// префикс "run" должен найти все ключи начинающиеся на "run"
	results, err := idx.SearchPrefix("run")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	// doc 1 (run от running), doc 2 (runner), doc 3 (run)
	// все три содержат слова с префиксом "run"
	// doc 4 не содержит
	if got.Contains(4) {
		t.Error("doc 4 (sleeping) should not match prefix 'run'")
	}
	if !got.Contains(1) || !got.Contains(3) {
		t.Error("docs 1 and 3 should match prefix 'run'")
	}
}

func TestSearchPrefix_NoMatch(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "hello world")

	results, err := idx.SearchPrefix("xyz")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestSearchPrefix_EmptyPrefix(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "hello world")

	// пустой префикс - ничего не ищем
	results, err := idx.SearchPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected no results for empty prefix, got %v", results)
	}
}

func TestSearchPrefix_ExactMatch(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "apple pie")
	_ = idx.IndexDocument(2, "application started")

	// префикс "appl" - после стемминга apple->appl, application->applic
	// значит "appl" как префикс должен покрыть оба
	results, err := idx.SearchPrefix("appl")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) || !got.Contains(2) {
		t.Errorf("expected docs 1 and 2, got %v", results)
	}
}

func TestSearchPrefix_CaseInsensitive(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "Hello world")

	// поиск с большой буквы должен работать
	results, err := idx.SearchPrefix("Hello")
	if err != nil {
		t.Fatal(err)
	}

	// "hello" после стемминга -> "hello", префикс "hello" должен найти
	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Errorf("expected doc 1, got %v", results)
	}
}

func TestSearchPrefix_Russian(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexerWithLang(db, "ru")

	_ = idx.IndexDocument(1, "бежать быстро")
	_ = idx.IndexDocument(2, "бегун победил")
	_ = idx.IndexDocument(3, "спать крепко")

	// "беж" и "бег" - стемы от бежать и бегун - оба начинаются на "бег" или "беж"
	// после стемминга: бежать->бежа, бегун->бегун
	// префикс "бег" найдет "бегун"
	results, err := idx.SearchPrefix("бег")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(2) {
		t.Errorf("expected doc 2 (бегун), got %v", results)
	}
	if got.Contains(3) {
		t.Error("doc 3 (спать) should not match")
	}
}
