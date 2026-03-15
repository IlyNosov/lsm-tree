package index

import (
	"testing"
	"time"
)

func date(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func datePtr(s string) *time.Time {
	t := date(s)
	return &t
}

func TestSearchDateRange_Basic(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// документы с разными датами
	_ = idx.IndexDocumentWithDate(1, "hello world", date("2024-01-15"))
	_ = idx.IndexDocumentWithDate(2, "foo bar", date("2024-03-20"))
	_ = idx.IndexDocumentWithDate(3, "test doc", date("2024-06-01"))
	_ = idx.IndexDocumentWithDate(4, "another one", date("2024-12-25"))

	// ищем январь-март 2024
	results, err := idx.SearchDateRange(date("2024-01-01"), date("2024-03-31"))
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) || !got.Contains(2) {
		t.Errorf("expected docs 1,2 in Jan-Mar range, got %v", results)
	}
	if got.Contains(3) || got.Contains(4) {
		t.Errorf("docs 3,4 should not be in Jan-Mar range, got %v", results)
	}
}

func TestSearchDateRange_SingleDay(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocumentWithDate(1, "today", date("2024-05-15"))
	_ = idx.IndexDocumentWithDate(2, "yesterday", date("2024-05-14"))

	results, err := idx.SearchDateRange(date("2024-05-15"), date("2024-05-15"))
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("expected doc 1")
	}
	if got.Contains(2) {
		t.Error("doc 2 should not match")
	}
}

func TestSearchDateRange_NoMatch(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)
	_ = idx.IndexDocumentWithDate(1, "old doc", date("2020-01-01"))

	results, err := idx.SearchDateRange(date("2024-01-01"), date("2024-12-31"))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Errorf("expected no results, got %v", results)
	}
}

func TestSearch_DateInBooleanQuery(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocumentWithDate(1, "quick fox", date("2024-01-15"))
	_ = idx.IndexDocumentWithDate(2, "quick dog", date("2024-06-20"))
	_ = idx.IndexDocumentWithDate(3, "lazy fox", date("2024-01-10"))

	// quick AND DATE(2024-01-01,2024-03-31) - быстрые документы из Q1
	results, err := idx.Search("quick AND DATE(2024-01-01,2024-03-31)")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) {
		t.Error("expected doc 1 (quick + Q1)")
	}
	if got.Contains(2) {
		t.Error("doc 2 is in Q2, should not match")
	}
	if got.Contains(3) {
		t.Error("doc 3 is lazy not quick")
	}
}

func TestSearch_DateOrWord(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocumentWithDate(1, "alpha", date("2024-01-15"))
	_ = idx.IndexDocumentWithDate(2, "beta", date("2024-06-20"))
	_ = idx.IndexDocumentWithDate(3, "gamma", date("2024-01-10"))

	// DATE(2024-01-01,2024-01-31) OR beta
	results, err := idx.Search("DATE(2024-01-01,2024-01-31) OR beta")
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	// doc 1 (январь), doc 3 (январь), doc 2 (beta)
	if !got.Contains(1) || !got.Contains(2) || !got.Contains(3) {
		t.Errorf("expected docs 1,2,3, got %v", results)
	}
}

func TestSearchAlive_Basic(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// doc 1: живет с 1 января по 1 марта
	_ = idx.IndexDocumentWithLifespan(1, "alpha", date("2024-01-01"), datePtr("2024-03-01"))
	// doc 2: живет с 1 февраля, бессрочный
	_ = idx.IndexDocumentWithLifespan(2, "beta", date("2024-02-01"), nil)
	// doc 3: живет с 1 января по 15 января
	_ = idx.IndexDocumentWithLifespan(3, "gamma", date("2024-01-01"), datePtr("2024-01-15"))

	// ищем живых на 20 января - 28 февраля
	results, err := idx.SearchAlive(date("2024-01-20"), date("2024-02-28"))
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	// doc 1: создан до 28 фев, истекает 1 марта >= 20 янв -> жив
	if !got.Contains(1) {
		t.Error("doc 1 should be alive (expires 2024-03-01 >= query start)")
	}
	// doc 2: создан 1 фев <= 28 фев, нет expires -> жив
	if !got.Contains(2) {
		t.Error("doc 2 should be alive (no expiry)")
	}
	// doc 3: истек 15 января < 20 января -> мертв
	if got.Contains(3) {
		t.Error("doc 3 should be dead (expired 2024-01-15 < query start 2024-01-20)")
	}
}

func TestSearchAlive_AllExpired(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocumentWithLifespan(1, "old", date("2020-01-01"), datePtr("2020-06-01"))
	_ = idx.IndexDocumentWithLifespan(2, "ancient", date("2019-01-01"), datePtr("2019-12-31"))

	results, err := idx.SearchAlive(date("2024-01-01"), date("2024-12-31"))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Errorf("expected no alive docs, got %v", results)
	}
}

func TestSearchAlive_NoExpiry(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	// все бессрочные
	_ = idx.IndexDocumentWithLifespan(1, "forever1", date("2024-01-01"), nil)
	_ = idx.IndexDocumentWithLifespan(2, "forever2", date("2024-06-01"), nil)

	results, err := idx.SearchAlive(date("2024-03-01"), date("2024-09-01"))
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	// doc 1: создан 1 янв <= 1 сен, без expires -> жив
	if !got.Contains(1) {
		t.Error("doc 1 should be alive")
	}
	// doc 2: создан 1 июня <= 1 сен, без expires -> жив
	if !got.Contains(2) {
		t.Error("doc 2 should be alive")
	}
}

func TestSearchCreatedInRange(t *testing.T) {
	db, cleanup := setupTestLSM(t)
	defer cleanup()

	idx := NewIndexer(db)

	_ = idx.IndexDocumentWithLifespan(1, "new", date("2024-03-15"), datePtr("2025-01-01"))
	_ = idx.IndexDocumentWithLifespan(2, "old", date("2023-06-01"), datePtr("2024-06-01"))
	_ = idx.IndexDocumentWithLifespan(3, "recent", date("2024-03-20"), nil)

	// документы появившиеся в марте 2024
	results, err := idx.SearchCreatedInRange(date("2024-03-01"), date("2024-03-31"))
	if err != nil {
		t.Fatal(err)
	}

	got := roaringOf(results...)
	if !got.Contains(1) || !got.Contains(3) {
		t.Errorf("expected docs 1,3, got %v", results)
	}
	if got.Contains(2) {
		t.Error("doc 2 was created in 2023, should not match")
	}
}

func TestParseDateRange(t *testing.T) {
	from, to, err := parseDateRange("2024-01-01,2024-12-31")
	if err != nil {
		t.Fatal(err)
	}
	if from.Format("2006-01-02") != "2024-01-01" {
		t.Errorf("from = %v, want 2024-01-01", from)
	}
	if to.Format("2006-01-02") != "2024-12-31" {
		t.Errorf("to = %v, want 2024-12-31", to)
	}

	// невалидный формат
	_, _, err = parseDateRange("garbage")
	if err == nil {
		t.Error("expected error for invalid date range")
	}
}
