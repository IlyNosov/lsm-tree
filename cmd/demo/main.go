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
