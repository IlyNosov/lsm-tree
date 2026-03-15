package index

import (
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// формат дат в ключах LSM
const dateFormat = "2006-01-02"

// префиксы для разных типов дат в LSM
const (
	prefixCreated = "__created__:" // дата создания документа
	prefixExpires = "__expires__:" // дата окончания жизни документа
)

// IndexDocumentWithDate индексирует документ с текстом и датой создания
func (idx *Indexer) IndexDocumentWithDate(docID uint32, text string, date time.Time) error {
	// индексируем текст как обычно
	if err := idx.IndexDocument(docID, text); err != nil {
		return err
	}

	// сохраняем дату создания в LSM
	return idx.putDateBitmap(prefixCreated, date, docID)
}

// IndexDocumentWithLifespan индексирует документ с датой создания и опциональной датой окончания
// expiresAt == nil означает что документ бессрочный
func (idx *Indexer) IndexDocumentWithLifespan(docID uint32, text string, createdAt time.Time, expiresAt *time.Time) error {
	if err := idx.IndexDocument(docID, text); err != nil {
		return err
	}

	// дата создания - всегда
	if err := idx.putDateBitmap(prefixCreated, createdAt, docID); err != nil {
		return err
	}

	// дата окончания - если задана
	if expiresAt != nil {
		if err := idx.putDateBitmap(prefixExpires, *expiresAt, docID); err != nil {
			return err
		}
	}

	return nil
}

// putDateBitmap записывает docID в битмап для конкретной даты
func (idx *Indexer) putDateBitmap(prefix string, date time.Time, docID uint32) error {
	key := prefix + date.Format(dateFormat)

	bm := roaring.New()
	bm.Add(docID)

	data, err := bitmapToBytes(bm)
	if err != nil {
		return err
	}

	return idx.lsm.Put(key, data)
}

// SearchDateRange ищет документы созданные в диапазоне дат [from, to] включительно
func (idx *Indexer) SearchDateRange(from, to time.Time) ([]uint32, error) {
	bm, err := idx.getDateRangeBitmap(prefixCreated, from, to)
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

// SearchCreatedInRange ищет документы появившиеся (созданные) в диапазоне дат
// по сути то же что SearchDateRange но явно названо для задания
func (idx *Indexer) SearchCreatedInRange(from, to time.Time) ([]uint32, error) {
	return idx.SearchDateRange(from, to)
}

// SearchAlive ищет документы валидные в заданном диапазоне дат
// документ валиден если:
// - создан до или в пределах диапазона (createdAt <= to)
// - ещё не истёк к началу диапазона (expiresAt >= from, или expiresAt не задан)
func (idx *Indexer) SearchAlive(from, to time.Time) ([]uint32, error) {
	// берем от начала времен до to
	createdBefore, err := idx.getDateRangeBitmap(prefixCreated, time.Time{}, to)
	if err != nil {
		return nil, err
	}

	// берем все expires от начала времен до (from - 1 день)
	dayBefore := from.AddDate(0, 0, -1)
	expiredBefore, err := idx.getDateRangeBitmap(prefixExpires, time.Time{}, dayBefore)
	if err != nil {
		return nil, err
	}

	// результат = созданные до to минус истёкшие до from
	// документы без expiresAt не попадут в expiredBefore, значит останутся
	result := roaring.AndNot(createdBefore, expiredBefore)

	return result.ToArray(), nil
}

// getDateRangeBitmap собирает OR битмапов для всех дат в диапазоне [from, to]
func (idx *Indexer) getDateRangeBitmap(prefix string, from, to time.Time) (*roaring.Bitmap, error) {
	startKey := prefix + from.Format(dateFormat)
	endKey := prefix + to.Format(dateFormat)

	// Range вернет все ключи __created__:YYYY-MM-DD в диапазоне
	rangeResult, err := idx.lsm.Range(startKey, endKey)
	if err != nil {
		return nil, err
	}

	result := roaring.New()

	for key := range rangeResult {
		// собираем полный битмап (из всех SSTable + memtable)
		bm, err := idx.getBitmap(key)
		if err != nil {
			return nil, err
		}
		result.Or(bm)
	}

	return result, nil
}

// GetDateBitmap возвращает битмап для диапазона дат - экспортированный метод для парсера
// нужен чтобы Search мог вычислять DATE(from,to) внутри булевых формул
func (idx *Indexer) GetDateBitmap(from, to time.Time) (*roaring.Bitmap, error) {
	return idx.getDateRangeBitmap(prefixCreated, from, to)
}

// parseDateRange парсит строку вида "2024-01-01,2024-12-31" в две даты
func parseDateRange(s string) (time.Time, time.Time, error) {
	// ожидаем формат "YYYY-MM-DD,YYYY-MM-DD"
	parts := splitDateRange(s)
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid date range: %q, expected YYYY-MM-DD,YYYY-MM-DD", s)
	}

	from, err := time.Parse(dateFormat, parts[0])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid from date: %w", err)
	}

	to, err := time.Parse(dateFormat, parts[1])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid to date: %w", err)
	}

	return from, to, nil
}

// splitDateRange разделяет "2024-01-01,2024-12-31" на два куска по запятой
// нельзя просто strings.Split потому что дата сама содержит дефисы
func splitDateRange(s string) []string {
	for i, c := range s {
		if c == ',' {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}
