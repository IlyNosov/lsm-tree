package bloom

import (
	"testing"
)

func TestBloomBasic(t *testing.T) {
	bf := New(1000, 0.01)
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		bf.Add(k)
	}
	for _, k := range keys {
		if !bf.MightContain(k) {
			t.Fatalf("expected key %s to be present in bloom", k)
		}
	}
	missKey := "not_exists"
	if bf.MightContain(missKey) {
		t.Log("false positive occurred (acceptable)")
	}
}

func TestBloomSerializeDeserialize(t *testing.T) {
	bf := New(100, 0.01)
	bf.Add("hello")
	bf.Add("world")

	data := bf.Serialize()

	bf2 := Deserialize(data)

	if !bf2.MightContain("hello") {
		t.Fatal("expected hello in bloom after deserialize")
	}
	if !bf2.MightContain("world") {
		t.Fatal("expected world in bloom after deserialize")
	}
}
