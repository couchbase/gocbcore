package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/opentracing/opentracing-go"
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

// GetCollectionManifest fetches the current server manifest. This function will not update the client's collection
// id cache.
func (agent *Agent) GetCollectionManifest(cb ManifestCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resp.Value, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetManifest,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback: handler,
	}
	return agent.dispatchOp(req)
}

// CollectionIdCallback is invoked upon completion of a GetCollectionID operation.
type CollectionIdCallback func(manifestID uint64, collectionID uint32, err error)

// GetCollectionIDOptions are the options available to the GetCollectionID command.
type GetCollectionIDOptions struct {
	TraceContext opentracing.SpanContext
}

// GetCollectionID fetches the collection id and manifest id that the collection belongs to, given a scope name
// and collection name. This function will also prime the client's collection id cache.
func (agent *Agent) GetCollectionID(scopeName string, collectionName string, opts GetCollectionIDOptions, cb CollectionIdCallback) (PendingOp, error) {
	tracer := agent.createOpTrace("GetCollectionID", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			tracer.Finish()
			cb(0, 0, err)
			return
		}

		manifestID := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

		agent.cidMgr.Add(collectionID, scopeName, collectionName)

		tracer.Finish()
		cb(manifestID, collectionID, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetID,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte(fmt.Sprintf("%s.%s", scopeName, collectionName)),
			Value:    nil,
			Vbucket:  0,
		},
		RootTraceContext: opts.TraceContext,
		ReplicaIdx:       -1,
	}

	req.Callback = handler

	return agent.dispatchOp(req)
}

func (cidMgr *collectionIdManager) createKey(scopeName, collectionName string) string {
	return fmt.Sprintf("%s.%s", scopeName, collectionName)
}

type collectionIdManager struct {
	idMap   map[string]uint32
	mapLock sync.Mutex
	agent   *Agent
}

func newCollectionIdManager(agent *Agent) *collectionIdManager {
	cidMgr := &collectionIdManager{
		agent: agent,
		idMap: make(map[string]uint32),
	}

	return cidMgr
}

func (cidMgr *collectionIdManager) Add(id uint32, scopeName, collectionName string) {
	key := cidMgr.createKey(scopeName, collectionName)
	cidMgr.mapLock.Lock()
	cidMgr.idMap[key] = id
	cidMgr.mapLock.Unlock()
}

func (cidMgr *collectionIdManager) Get(scopeName, collectionName string) (uint32, bool) {
	if scopeName == "_default" && collectionName == "_default" {
		return 0, true
	}

	cidMgr.mapLock.Lock()
	id, ok := cidMgr.idMap[cidMgr.createKey(scopeName, collectionName)]
	cidMgr.mapLock.Unlock()
	if !ok {
		return 0, false
	}

	return id, true
}

func (cidMgr *collectionIdManager) Remove(scopeName, collectionName string) {
	cidMgr.mapLock.Lock()
	delete(cidMgr.idMap, cidMgr.createKey(scopeName, collectionName))
	cidMgr.mapLock.Unlock()
}

type collectionPendingOp struct {
	pendingOp PendingOp
	currentOp PendingOp
	lock      sync.Mutex
	cancelled bool
}

func (op *collectionPendingOp) Cancel() bool {
	op.lock.Lock()
	op.cancelled = true
	pendingOp := op.pendingOp
	currentOp := op.currentOp
	op.lock.Unlock()

	if pendingOp != nil {
		pendingOp.Cancel()
	}

	if currentOp != nil {
		return currentOp.Cancel()
	}

	return false
}

func (cidMgr *collectionIdManager) dispatch(req *memdQRequest) (PendingOp, error) {
	if !cidMgr.agent.HasCollectionsSupport() {
		if (req.CollectionName != "" || req.ScopeName != "") && (req.CollectionName != "_default" || req.ScopeName != "_default") {
			return nil, ErrCollectionsUnsupported
		}
		return cidMgr.agent.dispatchOp(req)
	}

	cid, ok := cidMgr.Get(req.ScopeName, req.CollectionName)
	if ok {
		req.CollectionID = cid
		return cidMgr.agent.dispatchOp(req)
	}

	collectionOp := collectionPendingOp{}
	op, err := cidMgr.agent.GetCollectionID(
		req.ScopeName,
		req.CollectionName,
		GetCollectionIDOptions{TraceContext: req.RootTraceContext},
		func(manifestID uint64, collectionID uint32, err error) {
			if err != nil {
				if err == ErrCollectionUnknown {
					cidMgr.Remove(req.ScopeName, req.CollectionName)
				}
				req.tryCallback(nil, err)
			}

			cidMgr.Add(collectionID, req.ScopeName, req.CollectionName)
			req.CollectionID = collectionID
			op, err := cidMgr.agent.dispatchOp(req)
			if err != nil {
				req.tryCallback(nil, err)
			}
			collectionOp.lock.Lock()
			collectionOp.currentOp = op
			collectionOp.pendingOp = nil
			collectionOp.lock.Unlock()
		},
	)
	if err != nil {
		return nil, err
	}
	collectionOp.lock.Lock()
	collectionOp.currentOp = op
	collectionOp.pendingOp = req
	collectionOp.lock.Unlock()

	return &collectionOp, nil

}
