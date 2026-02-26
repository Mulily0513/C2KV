package db

import (
	"bytes"
	"testing"

	"github.com/Mulily0513/C2KV/internal/db/marshal"
)

func testMergeKV(key string, index uint64, ts int64, val string, typ int8) *marshal.KV {
	return &marshal.KV{
		Key: []byte(key),
		Data: &marshal.Data{
			Index:     index,
			TimeStamp: ts,
			Type:      typ,
			Value:     []byte(val),
		},
	}
}

func TestMergeSortedKVSourcesDedupLatest(t *testing.T) {
	sources := [][]*marshal.KV{
		{
			testMergeKV("a", 10, 100, "old-a", marshal.TypeInsert),
			testMergeKV("c", 11, 101, "c", marshal.TypeInsert),
		},
		{
			testMergeKV("a", 12, 110, "new-a", marshal.TypeInsert),
			testMergeKV("b", 13, 120, "", marshal.TypeDelete),
		},
		{
			testMergeKV("d", 14, 130, "d", marshal.TypeInsert),
		},
	}

	got := mergeSortedKVSources(sources, true)
	if len(got) != 4 {
		t.Fatalf("unexpected merged size, got=%d want=4", len(got))
	}
	if !bytes.Equal(got[0].Key, []byte("a")) || string(got[0].Data.Value) != "new-a" || got[0].Data.Index != 12 {
		t.Fatalf("unexpected latest value for key a: %+v", got[0].Data)
	}
	if !bytes.Equal(got[1].Key, []byte("b")) || got[1].Data.Type != marshal.TypeDelete {
		t.Fatalf("unexpected key b result: key=%s type=%d", string(got[1].Key), got[1].Data.Type)
	}
	if !bytes.Equal(got[2].Key, []byte("c")) || !bytes.Equal(got[3].Key, []byte("d")) {
		t.Fatalf("unexpected order after merge: %s %s", string(got[2].Key), string(got[3].Key))
	}
}

func TestMergeSortedKVSourcesNoDedup(t *testing.T) {
	sources := [][]*marshal.KV{
		{
			testMergeKV("a", 1, 10, "a1", marshal.TypeInsert),
			testMergeKV("c", 1, 10, "c1", marshal.TypeInsert),
		},
		{
			testMergeKV("b", 1, 10, "b1", marshal.TypeInsert),
			testMergeKV("d", 1, 10, "d1", marshal.TypeInsert),
		},
	}

	got := mergeSortedKVSources(sources, false)
	if len(got) != 4 {
		t.Fatalf("unexpected merged size, got=%d want=4", len(got))
	}
	want := []string{"a", "b", "c", "d"}
	for i := range want {
		if string(got[i].Key) != want[i] {
			t.Fatalf("unexpected order at %d, got=%s want=%s", i, string(got[i].Key), want[i])
		}
	}
}

func TestMergeSortedKVSourcesSkipInvalid(t *testing.T) {
	sources := [][]*marshal.KV{
		{
			nil,
			{Key: []byte("a")},
			testMergeKV("b", 1, 1, "b", marshal.TypeInsert),
		},
	}

	got := mergeSortedKVSources(sources, true)
	if len(got) != 1 || string(got[0].Key) != "b" {
		t.Fatalf("unexpected merge result: %+v", got)
	}
}
