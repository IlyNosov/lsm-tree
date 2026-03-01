package lsm

type CompactionMode int

const (
	CompactionKVLatest CompactionMode = iota
	CompactionBitmapOR
)

type Options struct {
	MemTableSize   int     // порог количества записей в оперативке, после которых она сбрасывается на диск
	BlockEntries   int     // кол-во записей в одном блоке SSTable
	BloomFP        float64 // ложно-положительный рейт в блум фильтре
	MaxL0          int     // сколько файлов находится в L0
	Dir            string  // директория хранения SSTable
	CompactionMode CompactionMode
}

func DefaultOptions(dir string) Options {
	return Options{
		MemTableSize:   1024,
		BlockEntries:   128,
		BloomFP:        0.01,
		MaxL0:          4,
		Dir:            dir,
		CompactionMode: CompactionKVLatest,
	}
}
