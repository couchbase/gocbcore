package gocbcore

import (
	"errors"
	"log"
)

var streamEndErrorMap = make(map[StreamEndStatus]error)

func makeStreamEndStatusError(code StreamEndStatus) error {
	err := errors.New(getKvStreamEndStatusText((code)))
	if streamEndErrorMap[code] != nil {
		log.Fatal("error handling setup failure")
	}
	streamEndErrorMap[code] = err
	return err
}

func getStreamEndStatusError(code StreamEndStatus) error {
	if err := streamEndErrorMap[code]; err != nil {
		return err
	}
	return errors.New(getKvStreamEndStatusText((code)))
}

var (
	// ErrDCPStreamClosed occurs when a DCP stream is closed gracefully.
	ErrDCPStreamClosed = makeStreamEndStatusError(streamEndClosed)

	// ErrDCPStreamStateChanged occurs when a DCP stream is interrupted by failover.
	ErrDCPStreamStateChanged = makeStreamEndStatusError(streamEndStateChanged)

	// ErrDCPStreamDisconnected occurs when a DCP stream is disconnected.
	ErrDCPStreamDisconnected = makeStreamEndStatusError(streamEndDisconnected)

	// ErrDCPStreamTooSlow occurs when a DCP stream is cancelled due to the application
	// not keeping up with the rate of flow of DCP events sent by the server.
	ErrDCPStreamTooSlow = makeStreamEndStatusError(streamEndTooSlow)

	// ErrDCPStreamFilterEmpty occurs when all of the collections for a DCP stream are
	// dropped.
	ErrDCPStreamFilterEmpty = makeStreamEndStatusError(streamEndFilterEmpty)
)
