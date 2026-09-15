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

	maxReplicaIdx := min(len(serverIdxChain), numReplicas+1)
	if maxReplicaIdx <= 1 {
		// This vBucket map entry contains no replicas, only the active (or nothing at all).
		return 0, errReplicaCurrentlyUnavailable
	}

	// We iterate through the _replicas_, ignoring the active, hence the '1 +' and the -1 in both operands of the
	// modulo operation.
	startReplicaIdx := 1 + (s.ReplicaIdx-1)%(maxReplicaIdx-1)
	candidateReplicaIdx := startReplicaIdx
	for {
		if serverIdxChain[candidateReplicaIdx] >= 0 {
			return candidateReplicaIdx, nil
		}

		candidateReplicaIdx = 1 + candidateReplicaIdx%(maxReplicaIdx-1)

		// If we've come back to the replica index we started from, no replica is available.
		if candidateReplicaIdx == startReplicaIdx {
			break
		}
	}

	return 0, errReplicaCurrentlyUnavailable
}
