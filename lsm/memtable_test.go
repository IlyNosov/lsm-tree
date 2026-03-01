package lsm

import (
	"testing"
)

func TestMemTablePutGet(t *testing.T) {
	m := NewMemTable()

	m.Put("a", []byte("1"), 1)

	e, ok := m.Get("a")
	if !ok {
		t.Fatal("expected key a")
	}
	if string(e.Value) != "1" {
		t.Fatal("wrong value")
	}
}

func TestMemTableDelete(t *testing.T) {
	m := NewMemTable()

	m.Put("a", []byte("1"), 1)
	m.Delete("a", 2)

	e, ok := m.Get("a")
	if !ok {
		t.Fatal("expected tombstone entry")
	}
	if !e.Tombstone {
		t.Fatal("expected tombstone")
	}
}

func TestMemTableSortedEntries(t *testing.T) {
	m := NewMemTable()

	m.Put("c", []byte("3"), 1)
	m.Put("a", []byte("1"), 2)
	m.Put("b", []byte("2"), 3)

	entries := m.SortedEntries()

	expected := []string{"a", "b", "c"}

	for i, e := range entries {
		if e.Key != expected[i] {
			t.Fatalf("expected %s, got %s", expected[i], e.Key)
		}
	}
}
