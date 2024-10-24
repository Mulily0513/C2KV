package code

import "errors"

var (
	// ErrRecordExists record with this key already exists.
	ErrRecordExists = errors.New("record with this key already exists")

	// ErrRecordExists record with this key already exists.
	ErrRecordNotExists = errors.New("record with this key not exists")

	// ErrRecordUpdated record was updated by another caller.
	ErrRecordUpdated = errors.New("record was updated by another caller")

	// ErrRecordDeleted record was deleted by another caller.
	ErrRecordDeleted = errors.New("record was deleted by another caller")

	ErrDBNotInit = errors.New("db is no init complete")

	ErrValueTooLarge = errors.New("the data size can't larger than segment size")

	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")

	ErrClosed = errors.New("the segment file is closed")

	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")

	ErrCanNotFondSSTFile = errors.New("can not found sst file")

	ErrIllegalMemTableNums = errors.New("requested index is unavailable due to compaction")
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")
