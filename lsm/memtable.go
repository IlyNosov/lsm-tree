package lsm

import (
	"lsm_tree/record"

	"github.com/emirpasic/gods/maps/treemap"
)

type MemTable struct {
	data *treemap.Map
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: treemap.NewWithStringComparator(),
	}
}

func (m *MemTable) Put(key string, value []byte, seq uint64) {
	v := make([]byte, len(value))
	copy(v, value)

	m.data.Put(key, record.Entry{
		Key:       key,
		Value:     v,
		Tombstone: false,
		Seq:       seq,
	})
}

func (m *MemTable) Delete(key string, seq uint64) {
	m.data.Put(key, record.Entry{
		Key:       key,
		Value:     nil,
		Tombstone: true,
		Seq:       seq,
	})
}

func (m *MemTable) Get(key string) (record.Entry, bool) {
	value, found := m.data.Get(key)
	if !found {
		return record.Entry{}, false
	}
	return value.(record.Entry), true
}

func (m *MemTable) Len() int {
	return m.data.Size()
}

func (m *MemTable) Reset() {
	// просто создаём новую структуру — это дешевле и чище
	m.data = treemap.NewWithStringComparator()
}

func (m *MemTable) SortedEntries() []record.Entry {
	if m.data.Size() == 0 {
		return nil
	}

	out := make([]record.Entry, 0, m.data.Size())
	it := m.data.Iterator()

	for it.Next() {
		out = append(out, it.Value().(record.Entry))
	}

	return out
}
