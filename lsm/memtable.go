package lsm

import (
	"lsm_tree/record"
	"sort"
)

type MemTable struct {
	data map[string]record.Entry
}

func NewMemTable() *MemTable {
	return &MemTable{data: make(map[string]record.Entry)}
}

func (m *MemTable) Put(key string, value []byte, seq uint64) {
	v := make([]byte, len(value))
	copy(v, value) // делаем копию, иначе значение в мемтейбл изменится

	m.data[key] = record.Entry{
		Key:       key,
		Value:     v,
		Tombstone: false,
		Seq:       seq,
	}
	// P.S. мы храним тут только последнюю версию ключа
}

func (m *MemTable) Delete(key string, seq uint64) {
	m.data[key] = record.Entry{
		Key:       key,
		Value:     nil,
		Tombstone: true, // т.к. SSTable иммутабельна, мы не сможем просто удалить из memtable значение, поскольку
		// при flush старое значение останется на диске, так что мы просто используем флаг
		Seq: seq,
	}
}

func (m *MemTable) Get(key string) (record.Entry, bool) {
	e, ok := m.data[key]
	return e, ok // возвращаем Entry без интерпретации, логика будет обрабатываться уже на уровне LSM
}

func (m *MemTable) Len() int {
	return len(m.data)
}

func (m *MemTable) Reset() {
	// очищаем старую мапу (если создавать новую будут излишние аллокации)
	for k := range m.data {
		delete(m.data, k)
	}
}

func (m *MemTable) SortedEntries() []record.Entry {
	// Memtable хранит данные неупорядоченно, а для SSTable нам нужна сортировка по ключу
	// Сложность тут O(n log n), но поскольку вызываем только при flush, должно быть допустимо
	if len(m.data) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]record.Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, m.data[k])
	}
	return out
}
