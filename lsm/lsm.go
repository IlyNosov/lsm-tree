package lsm

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"

	"lsm_tree/lsm/compaction"
	"lsm_tree/lsm/sstable"
	"lsm_tree/record"

	"github.com/RoaringBitmap/roaring"
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
	l := &LSM{
		opts: opts,
		mem:  NewMemTable(),
		l0:   []*sstable.SSTable{},
	}

	// создать директорию
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	// восстанавливаем состояние из существующих SSTable
	files, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}

	var maxID int
	for _, f := range files {
		if filepath.Ext(f.Name()) != ".sst" {
			continue
		}

		path := filepath.Join(opts.Dir, f.Name())

		tbl, err := sstable.Open(path)
		if err != nil {
			return nil, err
		}

		l.l0 = append(l.l0, tbl)

		var id int
		fmt.Sscanf(f.Name(), "l0-%06d.sst", &id)
		if id > maxID {
			maxID = id
		}
	}

	l.nextFileID = uint64(maxID + 1)

	return l, nil
}

// Put описывает значение по ключу
func (l *LSM) Put(key string, value []byte) error {
	l.seq++

	// объединяем дельты в memtable в индексном
	if l.opts.CompactionMode == CompactionBitmapOR {
		if e, ok := l.mem.Get(key); ok && !e.Tombstone && len(e.Value) > 0 {
			merged, err := orBitmapBytes(e.Value, value)
			if err != nil {
				return err
			}
			l.mem.Put(key, merged, l.seq)
		} else {
			l.mem.Put(key, value, l.seq)
		}
	} else {
		l.mem.Put(key, value, l.seq)
	}

	if l.mem.Len() >= l.opts.MemTableSize {
		return l.flush()
	}
	return nil
}

func orBitmapBytes(a, b []byte) ([]byte, error) {
	bmA := roaring.New()
	if err := bmA.UnmarshalBinary(a); err != nil {
		return nil, err
	}
	bmB := roaring.New()
	if err := bmB.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	bmA.Or(bmB)
	return bmA.MarshalBinary()
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
	if l.mem.Len() > 0 {
		if err := l.flush(); err != nil {
			return err
		}
	}

	for _, t := range l.l0 {
		_ = t.Close()
	}
	return nil
}

// flush сбрасывает memtable в новый SSTable файл
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

	// выбор merge
	switch l.opts.CompactionMode {
	case CompactionBitmapOR:
		if err := compaction.MergeToBuilderBitmapOR(l.l0, b); err != nil {
			return err
		}
	default:
		if err := compaction.MergeToBuilder(l.l0, b); err != nil {
			return err
		}
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

	// SSTables — используем Seek для логарифмического позиционирования
	for i, t := range l.l0 {
		it, err := t.NewIterator()
		if err != nil {
			return nil, err
		}

		// Seek использует бинарный поиск по индексу блоков,
		// а не линейное сканирование всех записей
		if err := it.Seek(start); err != nil {
			return nil, err
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

func (l *LSM) GetAllValues(key string) ([][]byte, error) {
	out := make([][]byte, 0, 1+len(l.l0))

	if e, ok := l.mem.Get(key); ok && !e.Tombstone && len(e.Value) > 0 {
		out = append(out, e.Value)
	}
	for i := len(l.l0) - 1; i >= 0; i-- {
		e, found, err := l.l0[i].Get(key)
		if err != nil {
			return nil, err
		}
		if found && !e.Tombstone && len(e.Value) > 0 {
			out = append(out, e.Value)
		}
	}
	return out, nil
}
