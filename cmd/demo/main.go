package main

import (
	"fmt"
	"log"
	"os"

	"lsm_tree/lsm"
)

func printFiles(dir string) {
	fmt.Println("SSTable files:")
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		fmt.Println("  ", f.Name())
	}
	fmt.Println()
}

func main() {
	dataDir := "./data"
	_ = os.RemoveAll(dataDir)

	opts := lsm.DefaultOptions(dataDir)

	// маленький размер, чтобы чаще был flush
	opts.MemTableSize = 3
	opts.BlockEntries = 2
	opts.MaxL0 = 3

	db, err := lsm.NewLSM(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== PHASE 1: initial inserts ===")

	db.Put("a", []byte("1"))
	db.Put("b", []byte("2"))
	db.Put("c", []byte("3")) // flush 1

	db.Put("d", []byte("4"))
	db.Put("e", []byte("5"))
	db.Put("f", []byte("6")) // flush 2

	db.Put("g", []byte("7"))
	db.Put("h", []byte("8"))
	db.Put("i", []byte("9")) // flush 3

	printFiles(dataDir)

	fmt.Println("=== PHASE 2: overwrite some keys ===")

	db.Put("a", []byte("100"))
	db.Put("b", []byte("200"))
	db.Put("c", []byte("300")) // flush 4

	printFiles(dataDir)

	fmt.Println("=== PHASE 3: deletes ===")

	db.Delete("d")
	db.Delete("e")
	db.Delete("f") // flush 5

	printFiles(dataDir)

	fmt.Println("=== PHASE 4: reads ===")

	keys := []string{"a", "b", "c", "d", "e", "f", "g"}

	for _, k := range keys {
		v, ok, err := db.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			fmt.Printf("%s = %s\n", k, string(v))
		} else {
			fmt.Printf("%s = <deleted or not found>\n", k)
		}
	}

	fmt.Println()
	fmt.Println("=== FINAL FILES ===")
	printFiles(dataDir)

	fmt.Println("Demo complete.")
}
