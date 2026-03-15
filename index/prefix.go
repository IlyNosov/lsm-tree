package index

import (
	"github.com/RoaringBitmap/roaring"
)

// SearchPrefix ищет документы, содержащие слова с заданным префиксом
// Например, префикс "run" найдет документы со словами "run", "running", "runner" и тд
//
// Идея:
// 1. нормализуем префикс (lowercase, но без стемминга - иначе потеряем смысл префикса)
// 2. делаем Range по LSM от prefix до prefix+"\xff" - это даст все ключи с этим префиксом
// 3. для каждого найденного ключа собираем битмап и ORим все вместе
func (idx *Indexer) SearchPrefix(prefix string) ([]uint32, error) {
	// приводим к нижнему регистру без стемминга
	prefix = toLowerClean(prefix)
	if prefix == "" {
		return nil, nil
	}

	// границы range - все ключи от prefix до prefix + максимальный символ
	// "\xff" гарантирует что мы захватим все ключи с этим префиксом
	start := prefix
	end := prefix + "\xff"

	// Range вернет map[key]value для всех ключей в диапазоне
	rangeResult, err := idx.lsm.Range(start, end)
	if err != nil {
		return nil, err
	}

	result := roaring.New()

	for key := range rangeResult {
		// пропускаем служебные ключи (типа __all_docs__)
		if len(key) > 0 && key[0] == '_' {
			continue
		}

		// собираем полный битмап для ключа (из всех SSTable + memtable)
		bm, err := idx.getBitmap(key)
		if err != nil {
			return nil, err
		}
		result.Or(bm)
	}

	return result.ToArray(), nil
}
