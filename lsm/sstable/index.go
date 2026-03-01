package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

// IndexEntry - один элемент индекса блока
type IndexEntry struct {
	FirstKey string // первый ключ в блоке
	Offset   int64  // позиция блока в файле SSTable
}

var (
	ErrCorruptedIndex = errors.New("corrupted index")
)

// BuildIndex строит индекс по уже сформированным блокам
func BuildIndex(blockFirstKeys []string, blockOffsets []int64) ([]IndexEntry, error) {
	if len(blockFirstKeys) != len(blockOffsets) {
		return nil, errors.New("blockFirstKeys and blockOffsets size mismatch")
	}

	idx := make([]IndexEntry, 0, len(blockFirstKeys))
	for i := 0; i < len(blockFirstKeys); i++ {
		idx = append(idx, IndexEntry{
			FirstKey: blockFirstKeys[i],
			Offset:   blockOffsets[i],
		})
	}
	// Предполагаем, что builder создаёт блоки в отсортированном порядке, поэтому idx уже отсортирован по FirstKey
	return idx, nil
}

// FindBlockOffset возвращает offset блока, который МОЖЕТ содержать данный ключ
func FindBlockOffset(index []IndexEntry, key string) (int64, bool) {
	if len(index) == 0 {
		return 0, false
	}

	// sort.Search находит первый i, где index[i].FirstKey > key
	// тогда нужный блок = i-1 (последний FirstKey <= key)
	i := sort.Search(len(index), func(i int) bool {
		return index[i].FirstKey > key
	}) - 1

	if i < 0 {
		// если key меньше первого FirstKey, то ключ не может быть в таблице
		return 0, false
	}

	return index[i].Offset, true
}

// SerializeIndex кодирует индекс в байты, чтобы сохранить в файл SSTable
func SerializeIndex(index []IndexEntry) ([]byte, error) {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(index))); err != nil {
		return nil, err
	}

	for _, e := range index {
		kb := []byte(e.FirstKey)

		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(kb))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(kb); err != nil {
			return nil, err
		}

		if err := binary.Write(&buf, binary.LittleEndian, e.Offset); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DeserializeIndex читает индекс из байтов
func DeserializeIndex(data []byte) ([]IndexEntry, error) {
	r := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, ErrCorruptedIndex
	}

	out := make([]IndexEntry, 0, count)

	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, ErrCorruptedIndex
		}
		if keyLen == 0 {
			return nil, ErrCorruptedIndex
		}

		kb := make([]byte, keyLen)
		if _, err := r.Read(kb); err != nil {
			return nil, ErrCorruptedIndex
		}

		var offset int64
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			return nil, ErrCorruptedIndex
		}

		out = append(out, IndexEntry{
			FirstKey: string(kb),
			Offset:   offset,
		})
	}

	return out, nil
}
