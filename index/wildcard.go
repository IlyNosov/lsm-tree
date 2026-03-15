package index

import (
	"path/filepath"
	"strings"

	"github.com/RoaringBitmap/roaring"
)

// SearchWildcard ищет документы по wildcard-паттерну используя k-gram индекс
// Поддерживает * (любое кол-во символов) и ? (один символ)
// Например: "run*", "r?n", "*ning", "r*ng"
//
// Алгоритм:
// 1. из паттерна извлекаем триграммы (пропуская * и ?)
// 2. пересекаем множества термов по каждой триграмме - получаем кандидатов
// 3. фильтруем кандидатов по glob-паттерну (точная проверка)
// 4. для прошедших фильтр собираем битмапы документов через OR
func (idx *Indexer) SearchWildcard(pattern string) ([]uint32, error) {
	pattern = strings.ToLower(pattern)
	if pattern == "" {
		return nil, nil
	}

	// извлекаем триграммы из паттерна (без учета wildcard символов)
	kgrams := extractKgramsFromPattern(pattern, 3)

	var candidates map[string]bool

	if len(kgrams) == 0 {
		// нет триграмм (короткий паттерн типа "r*" или "??")
		// перебираем все термы из kgram индекса
		candidates = idx.allTerms()
	} else {
		// пересекаем множества термов для каждой триграммы
		candidates = idx.intersectKgramSets(kgrams)
	}

	// фильтруем кандидатов по glob паттерну
	result := roaring.New()
	for term := range candidates {
		matched, _ := filepath.Match(pattern, term)
		if matched {
			bm, err := idx.getBitmap(term)
			if err != nil {
				return nil, err
			}
			result.Or(bm)
		}
	}

	return result.ToArray(), nil
}

// extractKgrams разбивает терм на k-граммы с маркерами границ
// "run" с k=3 -> ["$ru", "run", "un$"]
func extractKgrams(term string, k int) []string {
	// оборачиваем маркерами начала/конца
	padded := "$" + term + "$"
	var grams []string
	for i := 0; i <= len(padded)-k; i++ {
		grams = append(grams, padded[i:i+k])
	}
	return grams
}

// extractKgramsFromPattern извлекает k-граммы из wildcard паттерна
// пропускает фрагменты содержащие * или ?
// "r*ning" -> триграммы из "ning" и тд
func extractKgramsFromPattern(pattern string, k int) []string {
	// оборачиваем маркерами как при индексации
	padded := "$" + pattern + "$"
	var grams []string
	for i := 0; i <= len(padded)-k; i++ {
		gram := padded[i : i+k]
		// пропускаем триграммы с wildcard символами
		if strings.ContainsAny(gram, "*?") {
			continue
		}
		grams = append(grams, gram)
	}
	return grams
}

// intersectKgramSets пересекает множества термов для списка триграмм
// если триграмма "run" -> {running, runner, run}
// и триграмма "unn" -> {running, runner, funny}
// то пересечение = {running, runner}
func (idx *Indexer) intersectKgramSets(kgrams []string) map[string]bool {
	if len(kgrams) == 0 {
		return nil
	}

	// начинаем с первой триграммы
	result := make(map[string]bool)
	first := idx.kgrams[kgrams[0]]
	for term := range first {
		result[term] = true
	}

	// пересекаем с остальными
	for _, kg := range kgrams[1:] {
		termSet := idx.kgrams[kg]
		for term := range result {
			if !termSet[term] {
				delete(result, term)
			}
		}
	}

	return result
}

// allTerms собирает все уникальные термы из k-gram индекса
// используется когда паттерн слишком короткий для извлечения триграмм
func (idx *Indexer) allTerms() map[string]bool {
	terms := make(map[string]bool)
	for _, termSet := range idx.kgrams {
		for term := range termSet {
			terms[term] = true
		}
	}
	return terms
}
