package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"lsm_tree/index"
	"lsm_tree/lsm"
)

const (
	DocsCount = 1000 // Тестировал и на 100к с частыми flush, все норм
	VocabSize = 50
)

func main() {
	rand.Seed(time.Now().UnixNano())

	dir := "stress_data"
	_ = os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	opts.MemTableSize = 5
	opts.MaxL0 = 3
	opts.CompactionMode = lsm.CompactionBitmapOR

	db, err := lsm.NewLSM(opts)
	if err != nil {
		log.Fatal(err)
	}

	idx := index.NewIndexer(db)

	// создаем словарь
	vocab := make([]string, VocabSize)
	for i := 0; i < VocabSize; i++ {
		vocab[i] = fmt.Sprintf("term%d", i)
	}

	// ground truth
	expected := make(map[string]map[uint32]bool)

	fmt.Println("Indexing documents...")

	for docID := uint32(1); docID <= DocsCount; docID++ {
		termsPerDoc := rand.Intn(5) + 1

		text := ""
		seen := make(map[string]bool)

		for i := 0; i < termsPerDoc; i++ {
			term := vocab[rand.Intn(VocabSize)]
			text += term + " "

			if !seen[term] {
				if expected[term] == nil {
					expected[term] = make(map[uint32]bool)
				}
				expected[term][docID] = true
				seen[term] = true
			}
		}

		if err := idx.IndexDocument(docID, text); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Initial verification...")

	verifyIndex(idx, expected)

	fmt.Println("Closing DB...")
	db.Close()

	fmt.Println("Reopening DB...")

	db2, err := lsm.NewLSM(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	idx2 := index.NewIndexer(db2)

	fmt.Println("Verification after restart...")
	verifyIndex(idx2, expected)

	fmt.Println("ALL CHECKS PASSED")

	// префикс и wildcard
	demoPrefixAndWildcard()
}

func demoPrefixAndWildcard() {
	fmt.Println("\nPrefix & Wildcard Demo")

	dir := "demo3_data"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	opts.MemTableSize = 5
	opts.MaxL0 = 3
	opts.CompactionMode = lsm.CompactionBitmapOR

	db, err := lsm.NewLSM(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	idx := index.NewIndexer(db)

	// индексируем документы с реальными словами
	docs := map[uint32]string{
		1: "running through the forest quickly",
		2: "the runner finished first in the race",
		3: "we run every single morning",
		4: "sleeping under the stars peacefully",
		5: "reading interesting books all day",
		6: "reloading the application after restart",
		7: "programming requires patience and fun",
	}

	for id, text := range docs {
		if err := idx.IndexDocument(id, text); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("\nДокументы:")
	for id := uint32(1); id <= 7; id++ {
		fmt.Printf("  doc %d: %q\n", id, docs[id])
	}

	// префиксный поиск
	fmt.Println("\n--- Prefix Search ---")

	prefixTests := []string{"run", "read", "sleep", "prog"}
	for _, p := range prefixTests {
		results, err := idx.SearchPrefix(p)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  prefix(%q) -> docs %v\n", p, results)
	}

	// wildcard поиск
	fmt.Println("\n--- Wildcard Search (k-gram) ---")

	wildcardTests := []string{"run*", "r?n", "*ing", "re*d", "f*n", "?un"}
	for _, w := range wildcardTests {
		results, err := idx.SearchWildcard(w)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  wildcard(%q) -> docs %v\n", w, results)
	}

	fmt.Println("\nPrefix & Wildcard demo DONE")

	// демо дат
	demoDates()
}

func demoDates() {
	fmt.Println("\n=== Dates Demo ===")

	dir := "demo4_data"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	opts.MemTableSize = 5
	opts.MaxL0 = 3
	opts.CompactionMode = lsm.CompactionBitmapOR

	db, err := lsm.NewLSM(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	idx := index.NewIndexer(db)

	d := func(s string) time.Time {
		t, _ := time.Parse("2006-01-02", s)
		return t
	}
	dp := func(s string) *time.Time {
		t := d(s)
		return &t
	}

	// документы с датами создания и опциональной датой окончания
	type doc struct {
		id      uint32
		text    string
		created string
		expires *string
	}

	e1 := "2024-06-01"
	e2 := "2024-02-15"
	e3 := "2024-09-01"

	docs := []doc{
		{1, "quick brown fox", "2024-01-10", nil},
		{2, "lazy sleeping dog", "2024-03-20", &e1},
		{3, "fast running cat", "2024-01-05", &e2},
		{4, "smart clever bird", "2024-07-15", nil},
		{5, "old wise turtle", "2024-05-01", &e3},
	}

	fmt.Println("\nДокументы:")
	for _, dc := range docs {
		exp := "бессрочный"
		if dc.expires != nil {
			exp = "до " + *dc.expires
		}
		fmt.Printf("  doc %d: %q  (создан %s, %s)\n", dc.id, dc.text, dc.created, exp)
	}

	for _, dc := range docs {
		var expiresAt *time.Time
		if dc.expires != nil {
			expiresAt = dp(*dc.expires)
		}
		if err := idx.IndexDocumentWithLifespan(dc.id, dc.text, d(dc.created), expiresAt); err != nil {
			log.Fatal(err)
		}
	}

	// поиск по диапазону дат
	fmt.Println("\n--- SearchDateRange ---")

	ranges := [][2]string{
		{"2024-01-01", "2024-01-31"},
		{"2024-01-01", "2024-06-30"},
		{"2024-07-01", "2024-12-31"},
	}
	for _, r := range ranges {
		results, err := idx.SearchDateRange(d(r[0]), d(r[1]))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  created in [%s, %s] -> docs %v\n", r[0], r[1], results)
	}

	// поиск живых документов
	fmt.Println("\n--- SearchAlive ---")

	aliveRanges := [][2]string{
		{"2024-02-01", "2024-02-28"},
		{"2024-04-01", "2024-04-30"},
		{"2024-08-01", "2024-08-31"},
	}
	for _, r := range aliveRanges {
		results, err := idx.SearchAlive(d(r[0]), d(r[1]))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  alive in [%s, %s] -> docs %v\n", r[0], r[1], results)
	}

	// DATE() в булевых формулах
	fmt.Println("\n--- Boolean queries with DATE() ---")

	boolQueries := []string{
		"quick AND DATE(2024-01-01,2024-06-30)",
		"DATE(2024-01-01,2024-03-31) OR smart",
		"DATE(2024-01-01,2024-12-31) AND NOT lazy",
	}
	for _, q := range boolQueries {
		results, err := idx.Search(q)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s -> docs %v\n", q, results)
	}

	fmt.Println("\nDates demo DONE")
}
func verifyIndex(idx *index.Indexer, expected map[string]map[uint32]bool) {
	for term, truth := range expected {
		results, err := idx.Search(term)
		if err != nil {
			log.Fatalf("search error for %s: %v", term, err)
		}

		if len(results) != len(truth) {
			log.Fatalf("term %s mismatch count: got=%d expected=%d",
				term, len(results), len(truth))
		}

		for _, id := range results {
			if !truth[id] {
				log.Fatalf("term %s lost docID %d", term, id)
			}
		}
	}
}
