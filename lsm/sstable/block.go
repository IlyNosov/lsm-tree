package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	core "lsm_tree/record"
	"sort"
)

// SSTable хранит данные блоками (по 4KB или 16KB)
// Это нужно, чтобы при поиске по ключу не читать весь файл, а прочитать небольшой блок
// Блок содержит отсортированный по ключам список Entry

var (
	ErrCorruptedBlock = errors.New("corrupted block")
)

// EncodeBlock кодирует набор записей в байты блока
func EncodeBlock(entries []core.Entry) ([]byte, error) {
	var buf bytes.Buffer

	// Пишем count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(entries))); err != nil {
		return nil, err
	}

	for _, e := range entries {
		keyBytes := []byte(e.Key)

		// keyLen + key
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		// tombstone
		var t byte = 0
		if e.Tombstone {
			t = 1
		}
		if err := buf.WriteByte(t); err != nil {
			return nil, err
		}

		// seq
		if err := binary.Write(&buf, binary.LittleEndian, e.Seq); err != nil {
			return nil, err
		}

		// value
		if e.Tombstone {
			// если стоит tombstone, значит valueLen=0, valueBytes отсутствуют
			if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
				return nil, err
			}
			continue
		}

		// valueLen + valueBytes
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(e.Value))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(e.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeBlock декодирует блок целиком в []Entry
func DecodeBlock(data []byte) ([]core.Entry, error) {
	r := bytes.NewReader(data)

	// count
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, ErrCorruptedBlock
	}

	out := make([]core.Entry, 0, count)

	for i := uint32(0); i < count; i++ {
		// keyLen
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, ErrCorruptedBlock
		}
		if keyLen == 0 {
			return nil, ErrCorruptedBlock
		}

		keyBytes := make([]byte, keyLen)
		if _, err := r.Read(keyBytes); err != nil {
			return nil, ErrCorruptedBlock
		}
		key := string(keyBytes)

		// tombstone
		t, err := r.ReadByte()
		if err != nil {
			return nil, ErrCorruptedBlock
		}
		tombstone := (t == 1)

		// seq
		var seq uint64
		if err := binary.Read(r, binary.LittleEndian, &seq); err != nil {
			return nil, ErrCorruptedBlock
		}

		// valueLen
		var valueLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return nil, ErrCorruptedBlock
		}

		var value []byte
		if valueLen > 0 {
			value = make([]byte, valueLen)
			if _, err := r.Read(value); err != nil {
				return nil, ErrCorruptedBlock
			}
		} else {
			value = nil
		}

		// Если tombstone=true, значение может быть nil
		out = append(out, core.Entry{
			Key:       key,
			Value:     value,
			Tombstone: tombstone,
			Seq:       seq,
		})
	}

	return out, nil
}

// Тут бинпоиск по ключу в уже декодированном блоке
// Возвращает Entry и true, если ключ найден
func FindInDecodedBlock(entries []core.Entry, key string) (core.Entry, bool) {
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].Key >= key
	})
	if i < len(entries) && entries[i].Key == key {
		return entries[i], true
	}
	return core.Entry{}, false
}

// FindInBlock сразу декодирует и запускает бинпоиск
func FindInBlock(data []byte, key string) (core.Entry, bool, error) {
	entries, err := DecodeBlock(data)
	if err != nil {
		return core.Entry{}, false, err
	}
	e, ok := FindInDecodedBlock(entries, key)
	return e, ok, nil
}
