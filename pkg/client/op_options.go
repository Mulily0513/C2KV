package client

import "bytes"

func WithRange(end string) OpOption {
	return func(op *Op) {
		op.end = []byte(end)
	}
}

func WithFromKey() OpOption {
	return func(op *Op) {
		op.end = noPrefixEnd
		op.isOptsWithFromKey = true
	}
}

func WithPrefix() OpOption {
	return func(op *Op) {
		op.end = getPrefix(op.key)
		op.isOptsWithPrefix = true
	}
}

func WithLimit(limit int64) OpOption {
	return func(op *Op) {
		op.limit = limit
	}
}

func WithSerializable() OpOption {
	return func(op *Op) {
		op.serializable = true
	}
}

func WithKeysOnly() OpOption {
	return func(op *Op) {
		op.keysOnly = true
	}
}

func WithCountOnly() OpOption {
	return func(op *Op) {
		op.countOnly = true
	}
}

func WithRev(rev int64) OpOption {
	return func(op *Op) {
		op.rev = rev
	}
}

func WithPrevKV() OpOption {
	return func(op *Op) {
		op.prevKV = true
	}
}

func getPrefix(key []byte) []byte {
	if len(key) == 0 {
		return noPrefixEnd
	}
	end := append([]byte(nil), key...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return bytes.Repeat([]byte{0xff}, len(key))
}
