package index

import (
	"testing"
)

func TestSearchWildcard_StarSuffix(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocument(1, "running fast")
	_ = idx.IndexDocument(2, "runner wins")
	_ = idx.IndexDocument(3, "sleeping well")

	// "run*" - все термы начинающиеся на "run"
	results, err := idx.SearchWildcard("run*")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("doc 1 (running->run) should match run*")
	}
	if !got.Contains(2) {
		t.Error("doc 2 (runner) should match run*")
	}
	if got.Contains(3) {
		t.Error("doc 3 (sleeping) should not match run*")
	}
}

func TestSearchWildcard_StarPrefix(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocument(1, "running fast")  // run, fast
	_ = idx.IndexDocument(2, "cunning plan")  // cun, plan
	_ = idx.IndexDocument(3, "sleeping well") // sleep, well

	// "*un" - все термы заканчивающиеся на "un" (после стемминга)
	// run -> "run" подходит, cun -> "cun" подходит
	results, err := idx.SearchWildcard("*un")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("doc 1 (run) should match *un")
	}
	if got.Contains(3) {
		t.Error("doc 3 (sleep/well) should not match *un")
	}
}

func TestSearchWildcard_QuestionMark(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocument(1, "run fast")  // run, fast
	_ = idx.IndexDocument(2, "ran away")  // ran, away
	_ = idx.IndexDocument(3, "fun times") // fun, time

	// "r?n" - три буквы, первая r, последняя n, середина любая
	results, err := idx.SearchWildcard("r?n")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("doc 1 (run) should match r?n")
	}
	if !got.Contains(2) {
		t.Error("doc 2 (ran) should match r?n")
	}
	if got.Contains(3) {
		t.Error("doc 3 (fun) should not match r?n")
	}
}

func TestSearchWildcard_MiddleStar(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocument(1, "reading books")  // read, book
	_ = idx.IndexDocument(2, "reloading page") // reload, page

	// "re*d" - начинается на re, заканчивается на d
	results, err := idx.SearchWildcard("re*d")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("doc 1 (read) should match re*d")
	}
	if !got.Contains(2) {
		t.Error("doc 2 (reload) should match re*d")
	}
}

func TestSearchWildcard_NoMatch(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "hello world")

	results, err := idx.SearchWildcard("xyz*")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestSearchWildcard_Empty(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "hello world")

	results, err := idx.SearchWildcard("")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Errorf("expected no results for empty pattern, got %v", results)
	}
}

func TestSearchWildcard_AllStar(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocument(1, "hello world")
	_ = idx.IndexDocument(2, "foo bar")

	// "*" - матчит все термы
	results, err := idx.SearchWildcard("*")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) || !got.Contains(2) {
		t.Errorf("expected all docs, got %v", results)
	}
}

func TestExtractKgrams(t *testing.T) {
	grams := extractKgrams("run", 3)
	expected := []string{"$ru", "run", "un$"}

	if len(grams) != len(expected) {
		t.Fatalf("expected %d grams, got %d: %v", len(expected), len(grams), grams)
	}
	for i, g := range grams {
		if g != expected[i] {
			t.Errorf("gram[%d] = %q, want %q", i, g, expected[i])
		}
	}
}

func TestExtractKgramsFromPattern(t *testing.T) {
	// "r*ning" -> пропускаем триграммы с *
	grams := extractKgramsFromPattern("r*ning", 3)

	// должны остаться только чистые триграммы без *
	for _, g := range grams {
		if len(g) != 3 {
			t.Errorf("gram %q has wrong length", g)
		}
	}

	// проверяем что "nin", "ing", "ng$" есть
	gramSet := make(map[string]bool)
	for _, g := range grams {
		gramSet[g] = true
	}
	if !gramSet["nin"] {
		t.Error("expected gram 'nin'")
	}
	if !gramSet["ing"] {
		t.Error("expected gram 'ing'")
	}
	if !gramSet["ng$"] {
		t.Error("expected gram 'ng$'")
	}
}
