package gocbcore

func (suite *UnitTestSuite) TestSelectReplicaByIndex() {
	type testCase struct {
		name           string
		replicaIdx     int
		wrap           bool
		numReplicas    int
		serverIdxChain []int

		expectedReplicaIdx int
		expectedErr        error
	}

	testCases := []testCase{
		{
			name:               "IndexWithinBoundsNoWrap",
			replicaIdx:         2,
			wrap:               false,
			numReplicas:        2,
			serverIdxChain:     []int{1, 0, 2},
			expectedReplicaIdx: 2,
		},
		{
			name:               "IndexWithinBoundsWithWrap",
			replicaIdx:         2,
			wrap:               true,
			numReplicas:        2,
			serverIdxChain:     []int{2, 1, 0},
			expectedReplicaIdx: 2,
		},
		{
			name:           "IndexOutOfBoundsNoWrap",
			replicaIdx:     4,
			wrap:           false,
			numReplicas:    2,
			serverIdxChain: []int{0, 1, 2},
			expectedErr:    ErrInvalidReplica,
		},
		{
			name:               "IndexOutOfBoundsWithWrap",
			replicaIdx:         4,
			wrap:               true,
			numReplicas:        2,
			serverIdxChain:     []int{0, 1, 2},
			expectedReplicaIdx: 2,
		},
		{
			name:               "IndexOutOfBoundsWithWrapAndUnavailableReplicas",
			replicaIdx:         7,
			wrap:               true,
			numReplicas:        3,
			serverIdxChain:     []int{0, 1, -1, 2},
			expectedReplicaIdx: 1,
		},
		{
			name:           "IndexIsActiveNoWrap",
			replicaIdx:     0,
			wrap:           false,
			numReplicas:    2,
			serverIdxChain: []int{0, 1, 2},
			expectedErr:    ErrInvalidReplica,
		},
		{
			name:           "IndexIsActiveWithWrap",
			replicaIdx:     0,
			wrap:           true,
			numReplicas:    2,
			serverIdxChain: []int{0, 1, 2},
			expectedErr:    ErrInvalidReplica,
		},
		{
			name:           "ReplicaIndexTemporarilyUnavailable",
			replicaIdx:     1,
			wrap:           false,
			numReplicas:    2,
			serverIdxChain: []int{0, -1, 1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:               "ReplicaIndexUnavailableWithWrap",
			replicaIdx:         1,
			wrap:               true,
			numReplicas:        2,
			serverIdxChain:     []int{0, -1, 1},
			expectedReplicaIdx: 2,
		},
		{
			name:               "ReplicaIndexWithWrapThreeReplicas",
			replicaIdx:         3,
			wrap:               true,
			numReplicas:        3,
			serverIdxChain:     []int{0, 1, -1, 2},
			expectedReplicaIdx: 3,
		},
		{
			name:               "ReplicaIndexUnavailableWithWrapThreeReplicas",
			replicaIdx:         3,
			wrap:               true,
			numReplicas:        3,
			serverIdxChain:     []int{0, 2, 1, -1},
			expectedReplicaIdx: 1,
		},
		{
			name:           "NoReplicaIndexesAreAvailable",
			replicaIdx:     1,
			wrap:           false,
			numReplicas:    2,
			serverIdxChain: []int{0, -1, -1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:           "NoReplicaIndexesAreAvailableWithWrap",
			replicaIdx:     2,
			wrap:           true,
			numReplicas:    2,
			serverIdxChain: []int{0, -1, -1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:           "ReplicasInVBucketMapFewerThanNumReplicas",
			replicaIdx:     2,
			wrap:           false,
			numReplicas:    2,
			serverIdxChain: []int{0, 1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:               "ReplicasInVBucketMapFewerThanNumReplicasWithWrap",
			replicaIdx:         2,
			wrap:               true,
			numReplicas:        2,
			serverIdxChain:     []int{0, 1},
			expectedReplicaIdx: 1,
		},
		{
			name:           "IndexIsMultipleOfChainLengthWithWrapNoReplicasAvailable",
			replicaIdx:     3,
			wrap:           true,
			numReplicas:    2,
			serverIdxChain: []int{0, -1, -1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:           "IndexIsMultipleOfChainLengthWithWrapNoReplicasAvailableThreeReplicas",
			replicaIdx:     4,
			wrap:           true,
			numReplicas:    3,
			serverIdxChain: []int{0, -1, -1, -1},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:               "IndexOutOfBoundsWithWrapMustNotSkipAvailableReplica",
			replicaIdx:         5,
			wrap:               true,
			numReplicas:        3,
			serverIdxChain:     []int{0, 2, -1, -1},
			expectedReplicaIdx: 1,
		},
		{
			name:           "NoReplicasInVBucketMapWithWrap",
			replicaIdx:     1,
			wrap:           true,
			numReplicas:    1,
			serverIdxChain: []int{0},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
		{
			name:           "EmptyVBucketMapEntryWithWrap",
			replicaIdx:     1,
			wrap:           true,
			numReplicas:    1,
			serverIdxChain: []int{},
			expectedErr:    ErrReplicaCurrentlyUnavailable,
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			res, err := IndexReplicaSelector{
				ReplicaIdx: tc.replicaIdx,
				Wrap:       tc.wrap,
			}.selectReplica(tc.numReplicas, tc.serverIdxChain)

			if tc.expectedErr != nil {
				suite.Assert().ErrorIs(err, tc.expectedErr)
				suite.Assert().Zero(res)
			} else {
				suite.Assert().NoError(err)
				suite.Assert().Equal(tc.expectedReplicaIdx, res)
			}
		})
	}
}
