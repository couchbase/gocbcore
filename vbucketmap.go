package gocbcore

type vbucketMap struct {
	entries     [][]int
	numReplicas int
}

func newVbucketMap(entries [][]int, numReplicas int) *vbucketMap {
	vbMap := vbucketMap{
		entries:     entries,
		numReplicas: numReplicas,
	}
	return &vbMap
}

func (vbMap vbucketMap) IsValid() bool {
	return len(vbMap.entries) > 0 && len(vbMap.entries[0]) > 0
}

func (vbMap vbucketMap) NumVbuckets() int {
	return len(vbMap.entries)
}

func (vbMap vbucketMap) NumReplicas() int {
	return vbMap.numReplicas
}

func (vbMap vbucketMap) VbucketByKey(key []byte) uint16 {
	return uint16(cbCrc(key) % uint32(len(vbMap.entries)))
}

func (vbMap vbucketMap) NodeByVbucket(vbID uint16, replicaID uint32) (int, error) {
	if vbID >= uint16(len(vbMap.entries)) {
		return 0, errInvalidVBucket
	}

	if replicaID >= uint32(len(vbMap.entries[vbID])) {
		return 0, errInvalidReplica
	}

	return vbMap.entries[vbID][replicaID], nil
}
