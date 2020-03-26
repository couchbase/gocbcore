package gocbcore

//
// type statsComponent struct {
// 	kvMux            *kvMux
// 	tracer           RequestTracer
// 	noRootTraceSpans bool
// 	bucket           string
// }
//
// func (sc *statsComponent) Stats(opts StatsOptions, cb StatsExCallback) (PendingOp, error) {
// 	tracer := sc.createOpTrace("StatsEx", opts.TraceContext)
//
// 	iter, err := agent.kvMux.PipelineIterator()
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	stats := make(map[string]SingleServerStats)
// 	var statsLock sync.Mutex
//
// 	op := new(multiPendingOp)
// 	op.isIdempotent = true
// 	var expected uint32
//
// 	pipelines := make([]*memdPipeline, 0)
//
// 	switch target := opts.Target.(type) {
// 	case nil:
// 		expected = uint32(iter.Len())
//
// 		for iter.Next() {
// 			pipelines = append(pipelines, iter.Pipeline())
// 		}
// 	case VBucketIDStatsTarget:
// 		expected = 1
//
// 		srvIdx, err := muxer.vbMap.NodeByVbucket(target.VbID, 0)
// 		if err != nil {
// 			return nil, err
// 		}
//
// 		pipelines = append(pipelines, muxer.GetPipeline(srvIdx))
// 	default:
// 		return nil, errInvalidArgument
// 	}
//
// 	opHandledLocked := func() {
// 		completed := op.IncrementCompletedOps()
// 		if expected-completed == 0 {
// 			tracer.Finish()
// 			cb(&StatsResult{
// 				Servers: stats,
// 			}, nil)
// 		}
// 	}
//
// 	if opts.RetryStrategy == nil {
// 		opts.RetryStrategy = agent.defaultRetryStrategy
// 	}
//
// 	for _, pipeline := range pipelines {
// 		serverAddress := pipeline.Address()
//
// 		handler := func(resp *memdQResponse, req *memdQRequest, err error) {
// 			statsLock.Lock()
// 			defer statsLock.Unlock()
//
// 			// Fetch the specific stats key for this server.  Creating a new entry
// 			// for the server if we did not previously have one.
// 			curStats, ok := stats[serverAddress]
// 			if !ok {
// 				stats[serverAddress] = SingleServerStats{
// 					Stats: make(map[string]string),
// 				}
// 				curStats = stats[serverAddress]
// 			}
//
// 			if err != nil {
// 				// Store the first (and hopefully only) error into the Error field of this
// 				// server's stats entry.
// 				if curStats.Error == nil {
// 					curStats.Error = err
// 				} else {
// 					logDebugf("Got additional error for stats: %s: %v", serverAddress, err)
// 				}
//
// 				// When an error occurs, we need to cancel our persistent op.  However, because
// 				// a previous error may already have cancelled this and then raced, we should
// 				// ensure only a single completion is counted.
// 				if req.internalCancel(err) {
// 					opHandledLocked()
// 				}
//
// 				return
// 			}
//
// 			// Check if the key length is zero.  This indicates that we have reached
// 			// the ending of the stats listing by this server.
// 			if len(resp.Key) == 0 {
// 				// As this is a persistent request, we must manually cancel it to remove
// 				// it from the pending ops list.  To ensure we do not race multiple cancels,
// 				// we only handle it as completed the one time cancellation succeeds.
// 				if req.internalCancel(err) {
// 					opHandledLocked()
// 				}
//
// 				return
// 			}
//
// 			// Add the stat for this server to the list of stats.
// 			curStats.Stats[string(resp.Key)] = string(resp.Value)
// 		}
//
// 		req := &memdQRequest{
// 			memdPacket: memdPacket{
// 				Magic:    reqMagic,
// 				Opcode:   cmdStat,
// 				Datatype: 0,
// 				Cas:      0,
// 				Key:      []byte(opts.Key),
// 				Value:    nil,
// 			},
// 			Persistent:       true,
// 			Callback:         handler,
// 			RootTraceContext: tracer.RootContext(),
// 			RetryStrategy:    opts.RetryStrategy,
// 		}
//
// 		curOp, err := agent.kvMux.DispatchDirectToAddress(req, serverAddress)
// 		if err != nil {
// 			statsLock.Lock()
// 			stats[serverAddress] = SingleServerStats{
// 				Error: err,
// 			}
// 			opHandledLocked()
// 			statsLock.Unlock()
//
// 			continue
// 		}
//
// 		op.ops = append(op.ops, curOp)
// 	}
//
// 	return op, nil
// }
