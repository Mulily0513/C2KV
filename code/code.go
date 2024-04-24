package code

import "errors"

const (
	NodeInIErr = "NodeInIErr"

	ClusterId   = "cluster-id"
	LocalId     = "local-id"
	LocalIP     = "local-ip"
	RemoteId    = "remote-peer-id"
	RemoteIp    = "remote-peer-ip"
	RemoteUrls  = "remote-peer-urls"
	RemoteReqId = "remote-peer-req-id"

	MessageProcErr = "MessageProcErr"

	FailedReadMessage = "FailedReadMessage"
	TickErr           = "TickErr"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")
