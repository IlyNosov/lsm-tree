package sstable

import (
	"errors"
	core "lsm_tree/record"
	"os"
	"sort"

	"lsm_tree/lsm/bloom"
)

// SSTable иммутабельная дисковая структура LSM
type SSTable struct {
	f      *os.File
	path   string
	footer Footer
	index  []IndexEntry
	bloom  *bloom.BloomFilter
}

var (
	ErrEmptySSTable = errors.New("sstable is empty or corrupted")
)

// Open открывает SSTable файл и загружает в память индекс + bloom
func Open(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Узнаем размер файла
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := st.Size()

	// Минимальный размер хотя бы FooterSize
	if size < FooterSize {
		_ = f.Close()
		return nil, ErrEmptySSTable
	}

	// Читаем footer
	footerBytes := make([]byte, FooterSize)
	_, err = f.ReadAt(footerBytes, size-FooterSize)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	footer, err := DeserializeFooter(footerBytes)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// проверки целостности
	if footer.IndexOffset < 0 || footer.IndexSize <= 0 || footer.BloomOffset < 0 || footer.BloomSize <= 0 {
		_ = f.Close()
		return nil, ErrEmptySSTable
	}

	// Читаем Index по смещению и размеру
	indexBytes := make([]byte, footer.IndexSize)
	_, err = f.ReadAt(indexBytes, footer.IndexOffset)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	index, err := DeserializeIndex(indexBytes)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(index) == 0 {
		_ = f.Close()
		return nil, ErrEmptySSTable
	}

	// Читаем Bloom по смещению и размеру
	bloomBytes := make([]byte, footer.BloomSize)
	_, err = f.ReadAt(bloomBytes, footer.BloomOffset)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	bf := bloom.Deserialize(bloomBytes)

	return &SSTable{
		f:      f,
		path:   path,
		footer: footer,
		index:  index,
		bloom:  bf,
	}, nil
}

func (t *SSTable) FileName() string {
	return t.path
}

// Close закрывает файл SSTable
func (t *SSTable) Close() error {
	if t.f == nil {
		return nil
	}
	return t.f.Close()
}

// Get ищет ключ в SSTable
func (t *SSTable) Get(key string) (core.Entry, bool, error) {
	// Bloom filter
	if t.bloom != nil && !t.bloom.MightContain(key) {
		return core.Entry{}, false, nil
	}

	// Находим индекс блока, который может содержать key
	blockIdx, ok := t.findBlockIndex(key)
	if !ok {
		// если key меньше первого ключа первого блока, значит точно нет
		return core.Entry{}, false, nil
	}

	// Вычисляем границы блока (offset и size)
	blockOffset := t.index[blockIdx].Offset
	blockEnd := t.blockEndOffset(blockIdx)
	if blockEnd <= blockOffset {
		return core.Entry{}, false, ErrCorruptedBlock
	}
	blockSize := blockEnd - blockOffset

	// Читаем блок с диска
	buf := make([]byte, blockSize)
	_, err := t.f.ReadAt(buf, blockOffset)
	if err != nil {
		return core.Entry{}, false, err
	}

	// Ищем ключ внутри блока
	e, found, err := FindInBlock(buf, key)
	if err != nil {
		return core.Entry{}, false, err
	}
	return e, found, nil
}

// findBlockIndex возвращает индекс блока в массиве t.index, который может содержать key.
func (t *SSTable) findBlockIndex(key string) (int, bool) {
	if len(t.index) == 0 {
		return 0, false
	}

	// sort.Search ищет первый i, где FirstKey > key
	// Тогда нужный блок i-1
	i := sort.Search(len(t.index), func(i int) bool {
		return t.index[i].FirstKey > key
	}) - 1

	if i < 0 {
		return 0, false
	}
	return i, true
}

// blockEndOffset вычисляет конец блока, чтобы понять длину блока
func (t *SSTable) blockEndOffset(blockIdx int) int64 {
	// Если следующий блок существует его Offset и есть конец текущего
	if blockIdx+1 < len(t.index) {
		return t.index[blockIdx+1].Offset
	}
	// Иначе текущий последний, его конец начало Index
	return t.footer.IndexOffset
}
