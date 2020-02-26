package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
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
	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resp.Value, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
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
		Callback:      handler,
		RetryStrategy: opts.RetryStrategy,
	}
	return agent.dispatchOp(req)
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
	tracer := agent.createOpTrace("GetCollectionID", opts.TraceContext)

	handler := func(resp *memdQResponse, req *memdQRequest, err error) {
		cidCache, ok := agent.cidMgr.get(scopeName, collectionName)
		if !ok {
			cidCache = agent.cidMgr.newCollectionIDCache()
			agent.cidMgr.add(cidCache, scopeName, collectionName)
		}

		if err != nil {
			cidCache.lock.Lock()
			cidCache.id = invalidCid
			cidCache.err = err
			cidCache.lock.Unlock()

			tracer.Finish()
			cb(0, 0, err)
			return
		}

		manifestID := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

		cidCache.lock.Lock()
		cidCache.id = collectionID
		cidCache.lock.Unlock()

		tracer.Finish()
		cb(manifestID, collectionID, nil)
	}

	if opts.RetryStrategy == nil {
		opts.RetryStrategy = agent.defaultRetryStrategy
	}

	keyScopeName := scopeName
	if keyScopeName == "" {
		keyScopeName = "_default"
	}
	keyCollectionName := collectionName
	if keyCollectionName == "" {
		keyCollectionName = "_default"
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdCollectionsGetID,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      []byte(fmt.Sprintf("%s.%s", keyScopeName, keyCollectionName)),
			Value:    nil,
			Vbucket:  0,
		},
		ReplicaIdx:       -1,
		RetryStrategy:    opts.RetryStrategy,
		RootTraceContext: opts.TraceContext,
	}

	req.Callback = handler

	return agent.dispatchOp(req)
}

func (cidMgr *collectionIDManager) createKey(scopeName, collectionName string) string {
	return fmt.Sprintf("%s.%s", scopeName, collectionName)
}

type collectionIDManager struct {
	idMap        map[string]*collectionIDCache
	mapLock      sync.Mutex
	agent        *Agent
	maxQueueSize int
}

func newCollectionIDManager(agent *Agent, maxQueueSize int) *collectionIDManager {
	cidMgr := &collectionIDManager{
		agent:        agent,
		idMap:        make(map[string]*collectionIDCache),
		maxQueueSize: maxQueueSize,
	}

	return cidMgr
}

func (cidMgr *collectionIDManager) add(id *collectionIDCache, scopeName, collectionName string) {
	key := cidMgr.createKey(scopeName, collectionName)
	cidMgr.mapLock.Lock()
	cidMgr.idMap[key] = id
	cidMgr.mapLock.Unlock()
}

func (cidMgr *collectionIDManager) get(scopeName, collectionName string) (*collectionIDCache, bool) {
	cidMgr.mapLock.Lock()
	id, ok := cidMgr.idMap[cidMgr.createKey(scopeName, collectionName)]
	cidMgr.mapLock.Unlock()
	if !ok {
		return nil, false
	}

	return id, true
}

func (cidMgr *collectionIDManager) remove(scopeName, collectionName string) {
	cidMgr.mapLock.Lock()
	delete(cidMgr.idMap, cidMgr.createKey(scopeName, collectionName))
	cidMgr.mapLock.Unlock()
}

func (cidMgr *collectionIDManager) newCollectionIDCache() *collectionIDCache {
	return &collectionIDCache{
		agent:        cidMgr.agent,
		maxQueueSize: cidMgr.maxQueueSize,
	}
}

type collectionIDCache struct {
	opQueue        *memdOpQueue
	id             uint32
	collectionName string
	scopeName      string
	agent          *Agent
	lock           sync.Mutex
	err            error
	maxQueueSize   int
}

func (cid *collectionIDCache) sendWithCid(req *memdQRequest) error {
	cid.lock.Lock()
	req.CollectionID = cid.id
	cid.lock.Unlock()
	return cid.agent.kvMux.DispatchDirect(req)
}

func (cid *collectionIDCache) rejectRequest(req *memdQRequest) error {
	return cid.err
}

func (cid *collectionIDCache) queueRequest(req *memdQRequest) error {
	return cid.opQueue.Push(req, cid.maxQueueSize)
}

func (cid *collectionIDCache) refreshCid(req *memdQRequest) error {
	err := cid.opQueue.Push(req, cid.maxQueueSize)
	if err != nil {
		return err
	}
	_, err = cid.agent.GetCollectionID(req.ScopeName, req.CollectionName, GetCollectionIDOptions{TraceContext: req.RootTraceContext},
		func(manifestID uint64, collectionID uint32, err error) {
			// GetCollectionID will handle updating the id cache so we don't need to do it here
			if err != nil {
				cid.opQueue.Close()
				cid.opQueue.Drain(func(request *memdQRequest) {
					request.tryCallback(nil, err)
				})
				cid.opQueue = nil
				return
			}

			cid.opQueue.Close()
			cid.opQueue.Drain(func(request *memdQRequest) {
				request.CollectionID = collectionID
				cid.agent.kvMux.RequeueDirect(request, false)
			})
		},
	)

	return err
}

func (cid *collectionIDCache) dispatch(req *memdQRequest) error {
	cid.lock.Lock()
	// if the cid is unknown then mark the request pending and refresh cid first
	// if it's pending then queue the request
	// if it's invalid then reject the request
	// otherwise send the request
	switch cid.id {
	case unknownCid:
		cid.id = pendingCid
		cid.opQueue = newMemdOpQueue()
		cid.lock.Unlock()
		return cid.refreshCid(req)
	case pendingCid:
		cid.lock.Unlock()
		return cid.queueRequest(req)
	case invalidCid:
		cid.lock.Unlock()
		return cid.rejectRequest(req)
	default:
		cid.lock.Unlock()
		return cid.sendWithCid(req)
	}
}

func (cidMgr *collectionIDManager) dispatch(req *memdQRequest) error {
	noCollection := req.CollectionName == "" && req.ScopeName == ""
	defaultCollection := req.CollectionName == "_default" && req.ScopeName == "_default"
	collectionIDPresent := req.CollectionID > 0

	if !cidMgr.agent.HasCollectionsSupport() {
		if !(noCollection || defaultCollection) || collectionIDPresent {
			return errCollectionsUnsupported
		}
		err := cidMgr.agent.kvMux.DispatchDirect(req)
		if err != nil {
			return err
		}

		return nil
	}

	if noCollection || defaultCollection || collectionIDPresent {
		err := cidMgr.agent.kvMux.DispatchDirect(req)
		if err != nil {
			return err
		}

		return nil
	}

	cidCache, ok := cidMgr.get(req.ScopeName, req.CollectionName)
	if !ok {
		cidCache = cidMgr.newCollectionIDCache()
		cidCache.id = unknownCid
		cidMgr.add(cidCache, req.ScopeName, req.CollectionName)
	}
	err := cidCache.dispatch(req)
	if err != nil {
		return err
	}
	return nil
}

func (cidMgr *collectionIDManager) requeue(req *memdQRequest) {
	cidCache, ok := cidMgr.get(req.ScopeName, req.CollectionName)
	if !ok {
		cidCache = cidMgr.newCollectionIDCache()
		cidCache.id = unknownCid
		cidMgr.add(cidCache, req.ScopeName, req.CollectionName)
	}
	cidCache.lock.Lock()
	if cidCache.id != unknownCid && cidCache.id != pendingCid && cidCache.id != invalidCid {
		cidCache.id = unknownCid
	}
	cidCache.lock.Unlock()

	err := cidCache.dispatch(req)
	if err != nil {
		req.tryCallback(nil, err)
	}
}
