package compaction

import (
	"container/heap"

	"lsm_tree/lsm/sstable"
	"lsm_tree/record"
)

type heapItem struct {
	e         record.Entry
	it        *sstable.Iterator
	tableRank int
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].e.Key != h[j].e.Key {
		return h[i].e.Key < h[j].e.Key
	}
	if h[i].e.Seq != h[j].e.Seq {
		return h[i].e.Seq > h[j].e.Seq
	}
	return h[i].tableRank > h[j].tableRank
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func MergeToBuilder(tables []*sstable.SSTable, b *sstable.Builder) error {
	h := &mergeHeap{}
	heap.Init(h)
	for i, t := range tables {
		it, err := t.NewIterator()
		if err != nil {
			return err
		}
		if it.Valid() {
			heap.Push(h, heapItem{
				e:         it.Entry(),
				it:        it,
				tableRank: i,
			})
		}
	}

	var lastKey string
	haveLast := false

	for h.Len() > 0 {
		x := heap.Pop(h).(heapItem)

		// для каждого key пишем только первый (самый новый по seq/rank)
		if !haveLast || x.e.Key != lastKey {
			haveLast = true
			lastKey = x.e.Key

			// tombstone не пишем в новый SSTable
			if !x.e.Tombstone {
				if err := b.Add(x.e); err != nil {
					return err
				}
			}
		}

		ok, err := x.it.Next()
		if err != nil {
			return err
		}
		if ok && x.it.Valid() {
			heap.Push(h, heapItem{
				e:         x.it.Entry(),
				it:        x.it,
				tableRank: x.tableRank,
			})
		}
	}

	return nil
}
