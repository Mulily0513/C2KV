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
	"github.com/Mulily0513/C2KV/src/code"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

type Iterator struct {
	list  *Skiplist
	arena *Arena
	nd    *node
	value uint64
}

func (it *Iterator) Init(list *Skiplist) {
	it.list = list
	it.arena = list.arena
	it.nd = nil
	it.value = 0
}

func (it *Iterator) Valid() bool { return it.nd != nil }

func (it *Iterator) Key() []byte {
	return it.nd.getKey(it.arena)
}

func (it *Iterator) Value() []byte {
	valOffset, valSize := decodeValue(it.value)
	return it.arena.GetBytes(valOffset, valSize)
}

func (it *Iterator) Next() {
	next := it.list.getNext(it.nd, 0)
	it.setNode(next, false)
}

func (it *Iterator) Prev() {
	prev := it.list.getPrev(it.nd, 0)
	it.setNode(prev, true)
}

func (it *Iterator) SeekToFirst() {
	it.setNode(it.list.getNext(it.list.head, 0), false)
}

func (it *Iterator) SeekToLast() {
	it.setNode(it.list.getPrev(it.list.tail, 0), true)
}

func (it *Iterator) setNode(nd *node, reverse bool) bool {
	var value uint64

	success := true
	for nd != nil {
		// Skip past deleted nodes.
		value = atomic.LoadUint64(&nd.value)
		if value != deletedVal {
			break
		}

		success = false
		if reverse {
			nd = it.list.getPrev(nd, 0)
		} else {
			nd = it.list.getNext(nd, 0)
		}
	}

	it.value = value
	it.nd = nd
	return success
}

func (it *Iterator) Put(key []byte, val []byte) error {
	if it.Seek(key) {
		return it.Set(val)
	}
	return it.put(key, val)
}

func (it *Iterator) Seek(key []byte) (found bool) {
	var next *node
	_, next, found = it.seekForBaseSplice(key)
	present := it.setNode(next, false)
	return found && present
}

func (it *Iterator) put(key []byte, val []byte) error {
	var spl [maxHeight]splice
	if it.seekForSplice(key, &spl) {
		return code.ErrRecordExists
	}

	if it.list.testing {
		// This code is for better testing of concurrent performance.
		//For example, when coroutine 1 executes this code, it will call the runtime.Gosched() function to yield the execution right to coroutine 2. During this time, coroutine 2 may modify the content of splice. Then, coroutine 1 regains the execution right and continues to execute the subsequent code.
		//By adding a delay, the chance for coroutine 2 to modify splice can be increased, thus better simulating the race condition in a concurrent environment.
		runtime.Gosched()
	}

	nd, height, err := it.list.newNode(key, val)
	if err != nil {
		return err
	}

	value := nd.value
	ndOffset := it.arena.GetPointerOffset(unsafe.Pointer(nd))

	var found bool
	for i := 0; i < int(height); i++ {
		prev := spl[i].prev
		next := spl[i].next

		if prev == nil {
			// New node increased the height of the skiplist, so assume that the
			// new level has not yet been populated.
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}

			prev = it.list.head
			next = it.list.tail
		}

		// +----------------+     +------------+     +----------------+
		// |      prev      |     |     nd     |     |      next      |
		// | prevNextOffset |---->|            |     |                |
		// |                |<----| prevOffset |     |                |
		// |                |     | nextOffset |---->|                |
		// |                |     |            |<----| nextPrevOffset |
		// +----------------+     +------------+     +----------------+
		//
		// 1. Initialize prevOffset and nextOffset to point to prev and next.
		// 2. CAS prevNextOffset to repoint from next to nd.
		// 3. CAS nextPrevOffset to repoint from prev to nd.
		for {
			prevOffset := it.arena.GetPointerOffset(unsafe.Pointer(prev))
			nextOffset := it.arena.GetPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			// Check whether next has an updated link to prev. If it does not,
			// that can mean one of two things:
			//   1. The thread that added the next node hasn't yet had a chance
			//      to add the prev link (but will shortly).
			//   2. Another thread has added a new node between prev and next.
			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				// Determine whether #1 or #2 is true by checking whether prev
				// is still pointing to next. As long as the atomic operations
				// have at least acquire/release semantics (no need for
				// sequential consistency), this works, as it is equivalent to
				// the "publication safety" pattern.
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					// Ok, case #1 is true, so help the other thread along by
					// updating the next node's prev link.
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				// Managed to insert nd between prev and next, so update the next
				// node's prev link and go to the next level.
				if it.list.testing {
					// Add delay to make it easier to test race between this thread
					// and another thread that sees the intermediate state between
					// setting next and setting prev.
					runtime.Gosched()
				}

				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			// CAS failed. We need to recompute prev and next. It is unlikely to
			// be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev
			// and next.
			prev, next, found = it.list.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("how can another thread have inserted a node at a non-base level?")
				}

				return code.ErrRecordExists
			}
		}
	}

	it.value = value
	it.nd = nd
	return nil
}

func (it *Iterator) Set(val []byte) error {
	newVal, err := it.list.allocVal(val)
	if err != nil {
		return err
	}

	err = it.trySetValue(newVal)
	for err == code.ErrRecordExists {
		err = it.trySetValue(newVal)
	}
	return nil
}

func (it *Iterator) trySetValue(new uint64) error {
	if !atomic.CompareAndSwapUint64(&it.nd.value, it.value, new) {
		old := atomic.LoadUint64(&it.nd.value)
		if old == deletedVal {
			return code.ErrRecordDeleted
		}

		it.value = old
		return code.ErrRecordUpdated
	}

	it.value = new
	return nil
}

func (it *Iterator) Get(key []byte) (value []byte, err error) {
	if it.Seek(key) {
		return it.Value(), nil
	}
	return nil, code.ErrRecordNotExists
}

func (it *Iterator) seekForSplice(key []byte, spl *[maxHeight]splice) (found bool) {
	var prev, next *node
	level := int(it.list.Height() - 1)
	prev = it.list.head

	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = it.list.tail
		}

		spl[level].init(prev, next)
		if level == 0 {
			break
		}
		level--
	}
	return
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	level := int(it.list.Height() - 1)
	prev = it.list.head

	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if found {
			break
		}
		if level == 0 {
			break
		}
		level--
	}
	return
}

func (it *Iterator) Size() int {
	return int(it.list.Size())
}
