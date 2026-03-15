package sstable

import (
	"lsm_tree/record"
	"sort"
)

// Iterator - последовательный итератор по записям SSTable в порядке ключей
// Он держит только один текущий блок
type Iterator struct {
	t        *SSTable
	blockIdx int
	entries  []record.Entry
	entryIdx int
	valid    bool
}

// NewIterator создает итератор и позиционирует на первую запись
// Если таблица пустая/повреждённая - valid=false.
func (t *SSTable) NewIterator() (*Iterator, error) {
	it := &Iterator{t: t}

	if len(t.index) == 0 {
		it.valid = false
		return it, nil
	}

	// грузим первый блок
	if err := it.loadBlock(0); err != nil {
		return nil, err
	}

	// Если блок внезапно пустой - попробуем найти следующий непустой блок
	for it.blockIdx < len(t.index) && len(it.entries) == 0 {
		next := it.blockIdx + 1
		if next >= len(t.index) {
			it.valid = false
			return it, nil
		}
		if err := it.loadBlock(next); err != nil {
			return nil, err
		}
	}

	it.entryIdx = 0
	it.valid = len(it.entries) > 0
	return it, nil
}

// Valid говорит, стоит ли итератор на валидной записи
func (it *Iterator) Valid() bool {
	return it.valid
}

// Entry возвращает текущую запись
// Вызывать только если Valid()==true
func (it *Iterator) Entry() record.Entry {
	return it.entries[it.entryIdx]
}

// Next сдвигает итератор на следующую запись
// Возвращает true, если после сдвига есть валидная запись
func (it *Iterator) Next() (bool, error) {
	if !it.valid {
		return false, nil
	}

	// следующая запись внутри текущего блока
	it.entryIdx++
	if it.entryIdx < len(it.entries) {
		return true, nil
	}

	// переходим на следующий блок
	for {
		it.blockIdx++
		if it.blockIdx >= len(it.t.index) {
			it.valid = false
			return false, nil
		}

		if err := it.loadBlock(it.blockIdx); err != nil {
			return false, err
		}

		if len(it.entries) > 0 {
			it.entryIdx = 0
			it.valid = true
			return true, nil
		}
		// если блок пустой - идем дальше
	}
}

// Seek позиционирует итератор на первую запись с ключом >= key.
// Использует бинарный поиск по индексу блоков (логарифмический),
// а затем бинарный поиск внутри блока.
func (it *Iterator) Seek(key string) error {
	// Находим блок, который может содержать key
	blockIdx, ok := it.t.findBlockIndex(key)
	if !ok {
		// key меньше первого ключа — позиционируемся на начало
		if err := it.loadBlock(0); err != nil {
			it.valid = false
			return err
		}
		it.valid = len(it.entries) > 0
		return nil
	}

	// Загружаем найденный блок
	if err := it.loadBlock(blockIdx); err != nil {
		it.valid = false
		return err
	}

	// Бинарный поиск внутри блока: первая запись >= key
	idx := sort.Search(len(it.entries), func(i int) bool {
		return it.entries[i].Key >= key
	})

	if idx < len(it.entries) {
		it.entryIdx = idx
		it.valid = true
		return nil
	}

	// Все записи в блоке < key — переходим на следующий блок
	for {
		blockIdx++
		if blockIdx >= len(it.t.index) {
			it.valid = false
			return nil
		}
		if err := it.loadBlock(blockIdx); err != nil {
			it.valid = false
			return err
		}
		if len(it.entries) > 0 {
			it.entryIdx = 0
			it.valid = true
			return nil
		}
	}
}

func (it *Iterator) loadBlock(blockIdx int) error {
	blockOffset := it.t.index[blockIdx].Offset
	blockEnd := it.t.blockEndOffset(blockIdx)
	blockSize := blockEnd - blockOffset

	// защита от поврежденного формата
	if blockSize <= 0 {
		it.entries = nil
		return ErrCorruptedBlock
	}

	buf := make([]byte, blockSize)
	_, err := it.t.f.ReadAt(buf, blockOffset)
	if err != nil {
		return err
	}

	entries, err := DecodeBlock(buf)
	if err != nil {
		return err
	}

	it.blockIdx = blockIdx
	it.entries = entries
	it.entryIdx = 0
	return nil
}
