package sstable

import (
	"encoding/binary"
	"errors"
)

// Footer лежит в конце файла SSTable, хранит смещения и размеры для Index и Bloom filter
type Footer struct {
	IndexOffset int64
	IndexSize   int64
	BloomOffset int64
	BloomSize   int64
}

const FooterSize = 8 * 4

// по сути получаем фиксированный размер в 32 байта, и точно знаем, что последние эти 32 байта несут в себе
// информацию о том, где лежит индекс

var ErrCorruptedFooter = errors.New("corrupted footer")

// SerializeFooter превращает Footer в []byte, чтобы записать его в конец файла
func SerializeFooter(f Footer) ([]byte, error) {
	buf := make([]byte, FooterSize)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(f.IndexOffset))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(f.IndexSize))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(f.BloomOffset))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(f.BloomSize))

	return buf, nil
}

// DeserializeFooter восстанавливает Footer из []byte
func DeserializeFooter(data []byte) (Footer, error) {
	if len(data) != FooterSize {
		return Footer{}, ErrCorruptedFooter
	}

	return Footer{
		IndexOffset: int64(binary.LittleEndian.Uint64(data[0:8])),
		IndexSize:   int64(binary.LittleEndian.Uint64(data[8:16])),
		BloomOffset: int64(binary.LittleEndian.Uint64(data[16:24])),
		BloomSize:   int64(binary.LittleEndian.Uint64(data[24:32])),
	}, nil
}
