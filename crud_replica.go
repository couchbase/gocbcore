package gocbcore

// ReplicaSelector controls which replica should be fetched
//
// Uncommitted: This API may change in the future.
type ReplicaSelector interface {
	selectReplica(numReplicas int, serverIdxChain []int) (int, error)
}

// IndexReplicaSelector specifies an index for the replica to be fetched
//
// Uncommitted: This API may change in the future.
type IndexReplicaSelector struct {
	ReplicaIdx int
	Wrap       bool
}

// numReplicas is the number of replicas configured on the bucket
func (s IndexReplicaSelector) selectReplica(numReplicas int, serverIdxChain []int) (int, error) {
	if numReplicas == 0 {
		// No replicas configured in this bucket
		return 0, errInvalidReplica
	}

	if s.ReplicaIdx <= 0 {
		// Valid replica indexes are positive (0 represents the active).
		return 0, errInvalidReplica
	}

	if !s.Wrap {
		if s.ReplicaIdx > numReplicas {
			return 0, errInvalidReplica
		}
		if s.ReplicaIdx >= len(serverIdxChain) || serverIdxChain[s.ReplicaIdx] < 0 {
			return 0, errReplicaCurrentlyUnavailable
		}
		return s.ReplicaIdx, nil
	}

	// Any replicas that are not available (-1 entry on the vBucket map) are ignored.
	availableReplicaIdxs := availableReplicaIndexes(numReplicas, serverIdxChain)
	if len(availableReplicaIdxs) == 0 {
		return 0, errReplicaCurrentlyUnavailable
	}

	// We pick the Nth (N=s.ReplicaIdx-1) *available* replica modulo the number of available replicas.
	return availableReplicaIdxs[(s.ReplicaIdx-1)%len(availableReplicaIdxs)], nil
}

func availableReplicaIndexes(numReplicas int, serverIdxChain []int) []int {
	if len(serverIdxChain) <= 1 {
		// No replicas, only active
		return nil
	}
	var out []int
	// If there are more replicas in the server chain that configured on the bucket (i.e. numReplicas), we ignore them
	for idx, serverIdx := range serverIdxChain[1:min(numReplicas+1, len(serverIdxChain))] {
		replicaIdx := idx + 1
		if serverIdx < 0 {
			// Replica currently unavailable
			continue
		}

		out = append(out, replicaIdx)
	}
	return out
}
