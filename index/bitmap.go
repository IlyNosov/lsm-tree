package index

import (
	"github.com/RoaringBitmap/roaring"
)

// bitmapToBytes сериализует битмап в []byte
func bitmapToBytes(bm *roaring.Bitmap) ([]byte, error) {
	return bm.ToBytes()
}

// bytesToBitmap восстанавливает битмап из []byte
func bytesToBitmap(data []byte) (*roaring.Bitmap, error) {
	bm := roaring.New()
	err := bm.UnmarshalBinary(data)
	return bm, err
}
