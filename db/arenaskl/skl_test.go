/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"encoding/binary"
	"fmt"
	"github.com/Mulily0513/C2KV/db/marshal"
	"github.com/Mulily0513/C2KV/db/mocks"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	count := 0

	var it Iterator
	it.Init(s)

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}

	return count
}

// length iterates over skiplist in reverse order to give exact size.
func lengthRev(s *Skiplist) int {
	count := 0

	var it Iterator
	it.Init(s)

	for it.SeekToLast(); it.Valid(); it.Prev() {
		count++
	}

	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	found := it.Seek(key)
	require.False(t, found)
	require.False(t, it.Valid())
}

func TestFull(t *testing.T) {
	l := NewSkiplist(NewArena(1000))

	var it Iterator
	it.Init(l)

	for i := 0; i < 100; i++ {
		err := it.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i))
		require.Nil(t, err)
	}

	value, err := it.Get([]byte(fmt.Sprintf("%05d", 1)))
	t.Log(string(value))
	require.Nil(t, err)
}

// TestBasic tests single-threaded seeks and sets, adds, and deletes.
func TestBasic(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try adding values.
	it.Put([]byte("key1"), val1)
	it.Put([]byte("key3"), val3)
	it.Put([]byte("key2"), val2)

	require.False(t, it.Seek([]byte("key")))

	require.True(t, it.Seek([]byte("key1")))
	require.EqualValues(t, "00042", it.Value())

	require.True(t, it.Seek([]byte("key2")))
	require.EqualValues(t, "00052", it.Value())

	require.True(t, it.Seek([]byte("key3")))
	require.EqualValues(t, "00062", it.Value())

	require.True(t, it.Seek([]byte("key2")))
	require.Nil(t, it.Set(val4))
	require.EqualValues(t, "00072", it.Value())
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)

			it.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)

			found := it.Seek([]byte(fmt.Sprintf("%05d", i)))
			require.True(t, found)
			require.EqualValues(t, newValue(i), it.Value())
		}(i)
	}
	wg.Wait()
	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestConcurrentOneKey will read while writing to one single key.
func TestConcurrentOneKey(t *testing.T) {
	const n = 100
	key := []byte("thekey")

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var it Iterator
			it.Init(l)
			it.Put(key, newValue(i))
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var it Iterator
			it.Init(l)
			if !it.Seek(key) {
				return
			}

			atomic.StoreInt32(&sawValue, 1)
			v, err := strconv.Atoi(string(it.Value()))
			require.NoError(t, err)
			require.True(t, 0 <= v && v < n)
		}()
	}
	wg.Wait()
	require.True(t, sawValue > 0)
	require.Equal(t, 1, length(l))
	require.Equal(t, 1, lengthRev(l))
}

func TestIteratorAdd(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	// Add nil key and value (treated same as empty).
	err := it.Put(nil, nil)
	require.Nil(t, err)
	require.EqualValues(t, []byte{}, it.Key())
	require.EqualValues(t, []byte{}, it.Value())

	// Add empty key and value (treated same as nil).
	err = it.Put([]byte{}, []byte{})
	require.Nil(t, err)
	require.EqualValues(t, []byte{}, it.Key())
	require.EqualValues(t, []byte{}, it.Value())

	// Add to empty list.
	err = it.Put([]byte("00002"), []byte("00002"))
	require.Nil(t, err)
	require.EqualValues(t, "00002", it.Value())

	// Add first element in non-empty list.
	err = it.Put([]byte("00001"), []byte("00001"))
	require.Nil(t, err)
	require.EqualValues(t, "00001", it.Value())

	// Add last element in non-empty list.
	err = it.Put([]byte("00004"), []byte("00004"))
	require.Nil(t, err)
	require.EqualValues(t, "00004", it.Value())

	// Add element in middle of list.
	err = it.Put([]byte("00003"), []byte("00003"))
	require.Nil(t, err)
	require.EqualValues(t, "00003", it.Value())

	// Try to add element that was previously deleted.
	it.Seek([]byte("00004"))
	err = it.Put([]byte("00004"), []byte("00004*"))
	require.Nil(t, err)
	require.EqualValues(t, []byte("00004*"), it.Value())

	require.Equal(t, 5, length(l))
	require.Equal(t, 5, lengthRev(l))
}

func TestIteratorSet(t *testing.T) {
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	var it2 Iterator
	it2.Init(l)

	// Set when iterator position is invalid.
	require.Panics(t, func() { it.Set([]byte("00001a")) })

	// Set new value.
	it.Put([]byte("00001"), []byte("00001a"))
	err := it.Set([]byte("00001b"))
	require.Nil(t, err)
	require.EqualValues(t, "00001b", it.Value())
}

// TestConcurrentAdd races between adding same nodes.
func TestConcurrentAdd(t *testing.T) {
	const n = 100

	// Set testing flag to make it easier to trigger unusual race conditions.
	l := NewSkiplist(NewArena(arenaSize))
	l.testing = true

	start := make([]sync.WaitGroup, n)
	end := make([]sync.WaitGroup, n)

	for i := 0; i < n; i++ {
		start[i].Add(1)
		end[i].Add(2)
	}

	for f := 0; f < 2; f++ {
		go func(id int) {
			var it Iterator
			it.Init(l)

			for i := 0; i < n; i++ {
				start[i].Wait()

				key := newValue(i)
				val := []byte(fmt.Sprintf("%d: %05d", id, i))
				if it.Put(key, val) == nil {
					it.Seek(key)
					require.EqualValues(t, val, it.Value())
				}

				end[i].Done()
			}
		}(f)
	}

	for i := 0; i < n; i++ {
		start[i].Done()
		end[i].Wait()
	}

	require.Equal(t, n, length(l))
	require.Equal(t, n, lengthRev(l))
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	for i := n - 1; i >= 0; i-- {
		it.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i))
	}

	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		require.EqualValues(t, newValue(i), it.Value())
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	for i := 0; i < n; i++ {
		it.Put([]byte(fmt.Sprintf("%05d", i)), newValue(i))
	}

	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.EqualValues(t, newValue(i), it.Value())
		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := NewSkiplist(NewArena(arenaSize))

	var it Iterator
	it.Init(l)

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		it.Put([]byte(fmt.Sprintf("%05d", i*10+1000)), newValue(v))
	}

	found := it.Seek([]byte(""))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	//require.EqualValues(t, 1000, it.Meta())

	found = it.Seek([]byte("01000"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01000", it.Value())
	//require.EqualValues(t, 1000, it.Meta())

	found = it.Seek([]byte("01005"))
	require.False(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Value())
	//require.EqualValues(t, 1010, it.Meta())

	found = it.Seek([]byte("01010"))
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "01010", it.Value())
	//require.EqualValues(t, 1010, it.Meta())

	found = it.Seek([]byte("99999"))
	require.False(t, found)
	require.False(t, it.Valid())

	// Test seek for empty key.
	it.Put(nil, nil)
	found = it.Seek(nil)
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())

	found = it.Seek([]byte{})
	require.True(t, found)
	require.True(t, it.Valid())
	require.EqualValues(t, "", it.Value())
}

func randomKey(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			l := NewSkiplist(NewArena(uint32((b.N + 2) * MaxNodeSize)))
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				var iter Iterator
				iter.Init(l)

				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					if rng.Float32() < readFrac {
						if iter.Seek(randomKey(rng)) {
							_ = iter.Value()
							count++
						}
					} else {
						iter.Put(randomKey(rng), value)
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey(rng))]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey(rng))] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

func TestBasicWrite(t *testing.T) {
	l := NewSkiplist(NewArena(100 * arenaSize))

	var it Iterator
	it.Init(l)

	kvs := mocks.KVS_RAND_35MB_HASDEL_UQKey
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}

	for _, kv := range bytesKvs {
		err := it.Put(kv.Key, kv.Value)
		if err != nil {
			t.Log(err)
		}
	}

	for _, kv := range kvs {
		if it.Seek(kv.Key) {
			data := marshal.DecodeData(it.Value())
			require.EqualValues(t, kv.Data, data)
		}
	}
}

func TestBasicConcurrentWrite(t *testing.T) {
	l := NewSkiplist(NewArena(100 * arenaSize))

	kvs := mocks.KVS_RAND_35MB_HASDEL_UQKey
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}

	parts := make([][]*marshal.BytesKV, 10)
	for i, kv := range bytesKvs {
		part := i % 10
		parts[part] = append(parts[part], kv)
	}

	wg := &sync.WaitGroup{}
	for _, part := range parts {
		wg.Add(1)
		go func(kvs []*marshal.BytesKV) {
			var it Iterator
			it.Init(l)
			for _, kv := range kvs {
				err := it.Put(kv.Key, kv.Value)
				if err != nil {
					t.Log(err)
					return
				}
			}
			wg.Done()
		}(part)
	}
	wg.Wait()

	var it Iterator
	it.Init(l)
	for _, kv := range kvs {
		if it.Seek(kv.Key) {
			data := marshal.DecodeData(it.Value())
			require.EqualValues(t, kv.Data, data)
		}
	}
}
