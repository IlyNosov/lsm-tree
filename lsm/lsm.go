package lsm

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"

	"lsm_tree/lsm/compaction"
	"lsm_tree/lsm/sstable"
	"lsm_tree/record"
)

type LSM struct {
	opts       Options
	seq        uint64 // глобальный номер операции
	mem        *MemTable
	l0         []*sstable.SSTable
	nextFileID uint64 // счетчик для уникальных имен файлов SSTable
}

type rangeItem struct {
	e         record.Entry
	sstIt     *sstable.Iterator
	memIt     *memIterator
	tableRank int
}

type rangeHeap []rangeItem

func (h rangeHeap) Len() int { return len(h) }

func (h rangeHeap) Less(i, j int) bool {
	// ключ по возрастанию
	if h[i].e.Key != h[j].e.Key {
		return h[i].e.Key < h[j].e.Key
	}
	// при равном ключе - сначала более новая версия
	if h[i].e.Seq != h[j].e.Seq {
		return h[i].e.Seq > h[j].e.Seq
	}
	// если seq равен - более новая таблица
	return h[i].tableRank > h[j].tableRank
}

func (h rangeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *rangeHeap) Push(x interface{}) {
	*h = append(*h, x.(rangeItem))
}

func (h *rangeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type memIterator struct {
	entries []record.Entry
	idx     int
}

func newMemIterator(entries []record.Entry) *memIterator {
	return &memIterator{
		entries: entries,
		idx:     0,
	}
}

func (it *memIterator) Valid() bool {
	return it.idx < len(it.entries)
}

func (it *memIterator) Entry() record.Entry {
	return it.entries[it.idx]
}

func (it *memIterator) Next() (bool, error) {
	it.idx++
	return it.Valid(), nil
}

func NewLSM(opts Options) (*LSM, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	return &LSM{
		opts:       opts,
		mem:        NewMemTable(),
		l0:         make([]*sstable.SSTable, 0),
		nextFileID: 1,
	}, nil
}

// Put описывает значение по ключу
func (l *LSM) Put(key string, value []byte) error {
	l.seq++
	l.mem.Put(key, value, l.seq)

	// Триггер flush по количеству записей в memtable
	if l.mem.Len() >= l.opts.MemTableSize {
		return l.flush()
	}
	return nil
}

// Delete tombstone удаление
func (l *LSM) Delete(key string) error {
	l.seq++
	l.mem.Delete(key, l.seq)

	if l.mem.Len() >= l.opts.MemTableSize {
		return l.flush()
	}
	return nil
}

// Get читает по ключу
// сначала memtable, потом SSTable
func (l *LSM) Get(key string) ([]byte, bool, error) {
	if e, ok := l.mem.Get(key); ok {
		if e.Tombstone {
			return nil, false, nil
		}
		return e.Value, true, nil
	}

	for i := len(l.l0) - 1; i >= 0; i-- {
		e, found, err := l.l0[i].Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if e.Tombstone {
				return nil, false, nil
			}
			return e.Value, true, nil
		}
	}

	return nil, false, nil
}

// Close закрывает все открытые SSTable файлы
func (l *LSM) Close() error {
	var firstErr error
	for _, t := range l.l0 {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.l0 = nil
	return firstErr
}

// flush сбрасывает memtable в новый SSTable файл (L0)
func (l *LSM) flush() error {
	entries := l.mem.SortedEntries()
	if len(entries) == 0 {
		l.mem.Reset()
		return nil
	}

	// Генерируем имя файла
	path := filepath.Join(l.opts.Dir, fmt.Sprintf("l0-%06d.sst", l.nextFileID))
	l.nextFileID++

	// Builder создаёт SSTable:
	// - режет на блоки по BlockEntries
	// - строит индекс
	// - строит bloom
	// - пишет footer
	b, err := sstable.NewBuilder(
		path,
		l.opts.BlockEntries,
		uint64(len(entries)),
		l.opts.BloomFP,
	)
	if err != nil {
		return err
	}

	if err := b.Build(entries); err != nil {
		return err
	}
	t, err := sstable.Open(path)
	if err != nil {
		return err
	}
	l.l0 = append(l.l0, t)
	if len(l.l0) > l.opts.MaxL0 {
		if err := l.compact(); err != nil {
			return err
		}
	}
	l.mem.Reset()
	return nil
}

func (l *LSM) compact() error {
	path := filepath.Join(l.opts.Dir, fmt.Sprintf("l0-%06d.sst", l.nextFileID))
	l.nextFileID++

	estimated := uint64(l.opts.MemTableSize) * uint64(len(l.l0)+1)

	b, err := sstable.NewBuilder(
		path,
		l.opts.BlockEntries,
		estimated,
		l.opts.BloomFP,
	)
	if err != nil {
		return err
	}

	// streaming merge
	if err := compaction.MergeToBuilder(l.l0, b); err != nil {
		return err
	}
	if err := b.Finish(); err != nil {
		return err
	}

	newTable, err := sstable.Open(path)
	if err != nil {
		return err
	}

	// Удаляем старые файлы
	for _, t := range l.l0 {
		name := t.FileName()
		_ = t.Close()
		_ = os.Remove(name)
	}

	l.l0 = []*sstable.SSTable{newTable}
	return nil
}

func (l *LSM) Range(start, end string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	h := &rangeHeap{}
	heap.Init(h)

	// Memtable
	memEntries := l.mem.SortedEntries()
	memIt := newMemIterator(memEntries)

	// проматываем до start
	for memIt.Valid() && memIt.Entry().Key < start {
		memIt.Next()
	}

	if memIt.Valid() {
		heap.Push(h, rangeItem{
			e:         memIt.Entry(),
			memIt:     memIt,
			tableRank: 1000000, // выше любого L0
		})
	}

	// SSTables
	for i, t := range l.l0 {
		it, err := t.NewIterator()
		if err != nil {
			return nil, err
		}

		for it.Valid() && it.Entry().Key < start {
			ok, err := it.Next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
		}

		if it.Valid() {
			heap.Push(h, rangeItem{
				e:         it.Entry(),
				sstIt:     it,
				tableRank: i,
			})
		}
	}

	lastKey := ""
	haveLast := false

	for h.Len() > 0 {
		item := heap.Pop(h).(rangeItem)

		if item.e.Key > end {
			break
		}

		if !haveLast || item.e.Key != lastKey {
			if !item.e.Tombstone {
				result[item.e.Key] = item.e.Value
			} else {
				delete(result, item.e.Key)
			}
			lastKey = item.e.Key
			haveLast = true
		}

		var ok bool
		var err error

		if item.memIt != nil {
			ok, err = item.memIt.Next()
			if ok {
				item.e = item.memIt.Entry()
				heap.Push(h, item)
			}
		} else {
			ok, err = item.sstIt.Next()
			if ok {
				item.e = item.sstIt.Entry()
				heap.Push(h, item)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
