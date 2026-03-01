package compaction

import (
	"container/heap"

	"lsm_tree/lsm/sstable"
	"lsm_tree/record"

	"github.com/RoaringBitmap/roaring"
)

func MergeToBuilderBitmapOR(tables []*sstable.SSTable, b *sstable.Builder) error {
	h := &mergeHeap{}
	heap.Init(h)

	for i, t := range tables {
		it, err := t.NewIterator()
		if err != nil {
			return err
		}
		if it.Valid() {
			heap.Push(h, heapItem{e: it.Entry(), it: it, tableRank: i})
		}
	}

	var curKey string
	var curBM *roaring.Bitmap

	flush := func() error {
		if curBM == nil {
			return nil
		}
		data, err := curBM.MarshalBinary()
		if err != nil {
			return err
		}
		return b.Add(record.Entry{
			Key:       curKey,
			Value:     data,
			Tombstone: false,
			// Seq не будем трогать
		})
	}

	for h.Len() > 0 {
		x := heap.Pop(h).(heapItem)

		if curBM == nil || x.e.Key != curKey {
			if err := flush(); err != nil {
				return err
			}
			curKey = x.e.Key
			curBM = roaring.New()
		}

		// tombstone можно игнорировать
		if !x.e.Tombstone && len(x.e.Value) > 0 {
			bm := roaring.New()
			if err := bm.UnmarshalBinary(x.e.Value); err != nil {
				return err
			}
			curBM.Or(bm)
		}

		ok, err := x.it.Next()
		if err != nil {
			return err
		}
		if ok && x.it.Valid() {
			heap.Push(h, heapItem{e: x.it.Entry(), it: x.it, tableRank: x.tableRank})
		}
	}

	return flush()
}
