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

	fmt.Println("ALL CHECKS PASSED ✔")
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
