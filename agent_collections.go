package gocbcore

import (
	"encoding/json"
	"strconv"
)

const (
	unknownCid = uint32(0xFFFFFFFF)
	pendingCid = uint32(0xFFFFFFFE)
	invalidCid = uint32(0xFFFFFFFD)
)

// ManifestCollection is the representation of a collection within a manifest.
type ManifestCollection struct {
	UID  uint32
	Name string
}

// UnmarshalJSON is a custom implementation of json unmarshaling.
func (item *ManifestCollection) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	item.UID = uint32(decUID)
	item.Name = decData.Name
	return nil
}

// ManifestScope is the representation of a scope within a manifest.
type ManifestScope struct {
	UID         uint32
	Name        string
	Collections []ManifestCollection
}

// UnmarshalJSON is a custom implementation of json unmarshaling.
func (item *ManifestScope) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID         string               `json:"uid"`
		Name        string               `json:"name"`
		Collections []ManifestCollection `json:"collections"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 32)
	if err != nil {
		return err
	}

	item.UID = uint32(decUID)
	item.Name = decData.Name
	item.Collections = decData.Collections
	return nil
}

// Manifest is the representation of a collections manifest.
type Manifest struct {
	UID    uint64
	Scopes []ManifestScope
}

// UnmarshalJSON is a custom implementation of json unmarshaling.
func (item *Manifest) UnmarshalJSON(data []byte) error {
	decData := struct {
		UID    string          `json:"uid"`
		Scopes []ManifestScope `json:"scopes"`
	}{}
	if err := json.Unmarshal(data, &decData); err != nil {
		return err
	}

	decUID, err := strconv.ParseUint(decData.UID, 16, 64)
	if err != nil {
		return err
	}

	item.UID = decUID
	item.Scopes = decData.Scopes
	return nil
}

// ManifestCallback is invoked upon completion of a GetCollectionManifest operation.
type ManifestCallback func(manifest []byte, err error)

// GetCollectionManifestOptions are the options available to the GetCollectionManifest command.
type GetCollectionManifestOptions struct {
	// Volatile: Tracer API is subject to change.
	TraceContext  RequestSpanContext
	RetryStrategy RetryStrategy
}

// GetCollectionManifest fetches the current server manifest. This function will not update the client's collection
// id cache.
func (agent *Agent) GetCollectionManifest(opts GetCollectionManifestOptions, cb ManifestCallback) (PendingOp, error) {
	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	return agent.cidMgr.GetCollectionManifest(opts, cb)
}

// CollectionIDCallback is invoked upon completion of a GetCollectionID operation.
type CollectionIDCallback func(manifestID uint64, collectionID uint32, err error)

// GetCollectionIDOptions are the options available to the GetCollectionID command.
type GetCollectionIDOptions struct {
	RetryStrategy RetryStrategy
	// Volatile: Tracer API is subject to change.
	TraceContext RequestSpanContext
}

// GetCollectionID fetches the collection id and manifest id that the collection belongs to, given a scope name
// and collection name. This function will also prime the client's collection id cache.
func (agent *Agent) GetCollectionID(scopeName string, collectionName string, opts GetCollectionIDOptions, cb CollectionIDCallback) (PendingOp, error) {
	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	return agent.cidMgr.GetCollectionID(scopeName, collectionName, opts, cb)
}
