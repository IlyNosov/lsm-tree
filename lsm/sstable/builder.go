package sstable

import (
	core "lsm_tree/record"
	"os"
	"path/filepath"

	"lsm_tree/lsm/bloom"
)

// Builder отвечает за создание нового SSTable файла
type Builder struct {
	file          *os.File
	blockEntries  int
	bloom         *bloom.BloomFilter
	index         []IndexEntry
	currentOffset int64
	currentBlock  []core.Entry
	finished      bool
}

// NewBuilder создает builder для нового файла
func NewBuilder(filePath string, blockEntries int, estimatedKeys uint64, fp float64) (*Builder, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return &Builder{
		file:         f,
		blockEntries: blockEntries,
		bloom:        bloom.New(estimatedKeys, fp),
		index:        make([]IndexEntry, 0),
		currentBlock: make([]core.Entry, 0, blockEntries),
	}, nil
}

// Build принимает отсортированные записи и создает SSTable
func (b *Builder) Build(entries []core.Entry) error {
	for _, e := range entries {
		if err := b.Add(e); err != nil {
			return err
		}
	}
	return b.Finish()
}

func (b *Builder) Add(e core.Entry) error {
	if b.finished {
		return os.ErrInvalid
	}

	b.bloom.Add(e.Key)
	b.currentBlock = append(b.currentBlock, e)

	if len(b.currentBlock) >= b.blockEntries {
		if err := b.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Builder) Finish() error {
	if b.finished {
		return nil
	}

	// дописываем остаток блока
	if err := b.flushBlock(); err != nil {
		return err
	}

	// пишем индекс
	indexOffset := b.currentOffset
	indexBytes, err := SerializeIndex(b.index)
	if err != nil {
		return err
	}

	n, err := b.file.Write(indexBytes)
	if err != nil {
		return err
	}
	b.currentOffset += int64(n)
	indexSize := int64(n)

	// пишем bloom
	bloomOffset := b.currentOffset
	bloomBytes := b.bloom.Serialize()

	n, err = b.file.Write(bloomBytes)
	if err != nil {
		return err
	}
	b.currentOffset += int64(n)
	bloomSize := int64(n)

	// footer
	footer := Footer{
		IndexOffset: indexOffset,
		IndexSize:   indexSize,
		BloomOffset: bloomOffset,
		BloomSize:   bloomSize,
	}

	footerBytes, err := SerializeFooter(footer)
	if err != nil {
		return err
	}

	if _, err := b.file.Write(footerBytes); err != nil {
		return err
	}

	b.finished = true
	return b.file.Close()
}

func (b *Builder) flushBlock() error {
	if len(b.currentBlock) == 0 {
		return nil
	}

	blockOffset := b.currentOffset

	data, err := EncodeBlock(b.currentBlock)
	if err != nil {
		return err
	}

	n, err := b.file.Write(data)
	if err != nil {
		return err
	}
	b.currentOffset += int64(n)

	b.index = append(b.index, IndexEntry{
		FirstKey: b.currentBlock[0].Key,
		Offset:   blockOffset,
	})

	// очищаем буфер блока без переаллокаций
	b.currentBlock = b.currentBlock[:0]
	return nil
}
