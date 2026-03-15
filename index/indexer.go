package index

import (
	"lsm_tree/lsm"

	"github.com/RoaringBitmap/roaring"
)

type Indexer struct {
	lsm        *lsm.LSM
	allDocsKey string
	lang       string
	kgrams     map[string]map[string]bool // k-gram -> множество термов (для wildcard поиска)
}

func NewIndexer(lsm *lsm.LSM) *Indexer {
	return &Indexer{
		lsm:        lsm,
		allDocsKey: "__all_docs__",
		lang:       "en",
		kgrams:     make(map[string]map[string]bool),
	}
}

func NewIndexerWithLang(lsm *lsm.LSM, lang string) *Indexer {
	return &Indexer{
		lsm:        lsm,
		allDocsKey: "__all_docs__",
		lang:       lang,
		kgrams:     make(map[string]map[string]bool),
	}
}

// IndexDocument добавляет документ в индекс
// Язык для стемминга берётся из idx.lang, чтобы индексация и поиск были согласованы
func (idx *Indexer) IndexDocument(docID uint32, text string) error {
	words := tokenize(text)
	uniqueWords := make(map[string]bool)

	delta := roaring.New()
	delta.Add(docID)

	deltaBytes, err := bitmapToBytes(delta)
	if err != nil {
		return err
	}

	for _, w := range words {
		norm, ok := normalizeWord(w, idx.lang)
		if !ok {
			continue
		}
		if uniqueWords[norm] {
			continue
		}
		uniqueWords[norm] = true

		if err := idx.lsm.Put(norm, deltaBytes); err != nil {
			return err
		}

		// пополняем k-gram индекс для wildcard поиска
		idx.addToKgrams(norm)
	}
	return idx.updateAllDocs(docID, true)
}

// updateAllDocs добавляет docID из битмапа всех документов
func (idx *Indexer) updateAllDocs(docID uint32, add bool) error {
	if !add {
		return nil
	}

	bm := roaring.New()
	bm.Add(docID)

	data, err := bitmapToBytes(bm)
	if err != nil {
		return err
	}
	return idx.lsm.Put(idx.allDocsKey, data)
}

// getBitmap возвращает битмап для заданного ключа
func (idx *Indexer) getBitmap(key string) (*roaring.Bitmap, error) {
	chunks, err := idx.lsm.GetAllValues(key)
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		return roaring.New(), nil
	}

	res := roaring.New()
	for _, data := range chunks {
		if len(data) == 0 {
			continue
		}
		bm, err := bytesToBitmap(data)
		if err != nil {
			return nil, err
		}
		res.Or(bm)
	}
	return res, nil
}

// getAllDocs возвращает битмап всех проиндексированных документов
func (idx *Indexer) getAllDocs() (*roaring.Bitmap, error) {
	return idx.getBitmap(idx.allDocsKey)
}

// addToKgrams разбивает терм на триграммы и добавляет в k-gram индекс
// терм оборачивается маркерами $ (начало) и $ (конец)
// например "run" -> "$ru", "run", "un$"
func (idx *Indexer) addToKgrams(term string) {
	for _, kg := range extractKgrams(term, 3) {
		if idx.kgrams[kg] == nil {
			idx.kgrams[kg] = make(map[string]bool)
		}
		idx.kgrams[kg][term] = true
	}
}
