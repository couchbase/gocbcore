package gocbcore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcore/v10/memd"
)

var (
	errPipelineClosed = errors.New("pipeline has been closed")
	errPipelineFull   = errors.New("pipeline is too full")
)

type memdGetClientFn func(cancelSig <-chan struct{}) (*memdClient, error)

type newMemdPipelineOptions struct {
	endpoint               routeEndpoint
	maxClients             int
	maxItems               int
	getClientFn            memdGetClientFn
	telemetry              *telemetryComponent
	onPipelineConnected    func()
	onPipelineDisconnected func()
}

type memdPipeline struct {
	address     string
	getClientFn memdGetClientFn
	maxItems    int
	queue       *memdOpQueue
	maxClients  int

	clients          []*memdPipelineClient
	clientsLock      sync.Mutex
	clientsConnected atomic.Uint32

	onPipelineConnected    func()
	onPipelineDisconnected func()

	isSeedNode       bool
	serverGroup      string
	nodeUUID         string
	canonicalAddress string
	telemetry        *telemetryComponent
}

func newPipeline(opts *newMemdPipelineOptions) *memdPipeline {
	return &memdPipeline{
		address:          opts.endpoint.Address,
		getClientFn:      opts.getClientFn,
		maxClients:       opts.maxClients,
		maxItems:         opts.maxItems,
		queue:            newMemdOpQueue(),
		isSeedNode:       opts.endpoint.IsSeedNode,
		serverGroup:      opts.endpoint.ServerGroup,
		nodeUUID:         opts.endpoint.NodeUUID,
		canonicalAddress: opts.endpoint.CanonicalAddress,
		telemetry:        opts.telemetry,

		onPipelineConnected:    opts.onPipelineConnected,
		onPipelineDisconnected: opts.onPipelineDisconnected,
	}
}

func newDeadPipeline(maxItems int) *memdPipeline {
	return newPipeline(&newMemdPipelineOptions{
		endpoint:               routeEndpoint{},
		maxClients:             0,
		maxItems:               maxItems,
		getClientFn:            nil,
		telemetry:              nil,
		onPipelineConnected:    nil,
		onPipelineDisconnected: nil,
	})
}

// nolint: unused
func (pipeline *memdPipeline) debugString() string {
	var outStr string

	if pipeline.address != "" {
		outStr += fmt.Sprintf("Address: %s\n", pipeline.address)
		outStr += fmt.Sprintf("Max Clients: %d\n", pipeline.maxClients)
		outStr += fmt.Sprintf("Num Clients: %d\n", len(pipeline.clients))
		outStr += fmt.Sprintf("Max Items: %d\n", pipeline.maxItems)
	} else {
		outStr += "Dead-Server Queue\n"
	}

	outStr += "Op Queue:\n"
	outStr += reindentLog("  ", pipeline.queue.debugString())

	return outStr
}

func (pipeline *memdPipeline) IsSeedNode() bool {
	return pipeline.isSeedNode
}

func (pipeline *memdPipeline) Clients() []*memdPipelineClient {
	pipeline.clientsLock.Lock()
	defer pipeline.clientsLock.Unlock()
	return pipeline.clients
}

func (pipeline *memdPipeline) SupportsFeature(feature memd.HelloFeature) bool {
	pipeline.clientsLock.Lock()
	defer pipeline.clientsLock.Unlock()
	if len(pipeline.clients) == 0 {
		return false
	}
	// If any of the connections do not support this feature then we consider it as unsupported.
	for _, cli := range pipeline.clients {
		if !cli.SupportsFeature(feature) {
			return false
		}
	}

	return true
}

func (pipeline *memdPipeline) Address() string {
	return pipeline.address
}

func (pipeline *memdPipeline) ServerGroup() string {
	return pipeline.serverGroup
}

func (pipeline *memdPipeline) StartClients() {
	pipeline.clientsLock.Lock()
	defer pipeline.clientsLock.Unlock()

	for len(pipeline.clients) < pipeline.maxClients {
		client := newMemdPipelineClient(pipeline, &newMemdPipelineClientOptions{
			onConnected:    pipeline.onClientConnected,
			onDisconnected: pipeline.onClientDisconnected,
		})
		pipeline.clients = append(pipeline.clients, client)

		go client.Run()
	}
}

func (pipeline *memdPipeline) onClientConnected() {
	clientsConnected := pipeline.clientsConnected.Add(1)
	if clientsConnected == 1 {
		pipeline.onPipelineConnected()
	}
}

func (pipeline *memdPipeline) onClientDisconnected() {
	clientsConnected := pipeline.clientsConnected.Add(^uint32(0))
	if clientsConnected == 0 {
		pipeline.onPipelineDisconnected()
	}
}

func (pipeline *memdPipeline) sendRequest(req *memdQRequest, maxItems int) error {
	if pipeline.telemetry.TelemetryEnabled() {
		cmdCategory := req.Command.Category()

		if cmdCategory != memd.CmdCategoryUnknown {
			var node, altNode string
			if pipeline.canonicalAddress != "" && pipeline.canonicalAddress != pipeline.address {
				var err error
				node, err = hostFromHostPort(pipeline.canonicalAddress)
				if err != nil {
					node = pipeline.canonicalAddress
				}
				altNode, err = hostFromHostPort(pipeline.address)
				if err != nil {
					altNode = pipeline.address
				}
			} else {
				var err error
				node, err = hostFromHostPort(pipeline.address)
				if err != nil {
					node = pipeline.address
				}
			}

			req.processingLock.Lock()
			req.telemetryRecorder = pipeline.telemetry.GetRecorder(telemetryOperationAttributes{
				node:     node,
				altNode:  altNode,
				nodeUUID: pipeline.nodeUUID,
				service:  MemdService,
				mutation: cmdCategory == memd.CmdCategoryMutation,
				durable:  req.DurabilityLevelFrame != nil && req.DurabilityLevelFrame.DurabilityLevel != 0,
			})
			req.processingLock.Unlock()
		}
	}

	err := pipeline.queue.Push(req, maxItems)
	if err == errOpQueueClosed {
		return errPipelineClosed
	} else if err == errOpQueueFull {
		return errPipelineFull
	} else if err != nil {
		return err
	}

	return nil
}

func (pipeline *memdPipeline) RequeueRequest(req *memdQRequest) error {
	return pipeline.sendRequest(req, 0)
}

func (pipeline *memdPipeline) SendRequest(req *memdQRequest) error {
	return pipeline.sendRequest(req, pipeline.maxItems)
}

// Takeover performs a takeover of another pipeline.  Note that this does not
// take over the requests queued in the old pipeline, and those must
// be drained and processed separately.
func (pipeline *memdPipeline) Takeover(oldPipeline *memdPipeline) {
	if oldPipeline.address != pipeline.address {
		logErrorf("Attempted pipeline takeover for differing address")

		// We try to 'gracefully' error here by resolving all the requests as
		//  errors, but allowing the application to continue.
		err := oldPipeline.Close()
		if err != nil {
			// Log and continue with this non-fatal error.
			logDebugf("Failed to shutdown old pipeline (%s)", err)
		}

		// Drain all the requests as an internal error so they are not lost
		oldPipeline.Drain(func(req *memdQRequest) {
			req.tryCallback(nil, errCliInternalError)
		})

		return
	}

	// Migrate all the clients to the new pipeline
	oldPipeline.clientsLock.Lock()
	clients := oldPipeline.clients
	oldPipeline.clients = nil
	oldPipeline.clientsLock.Unlock()

	pipeline.clientsLock.Lock()
	pipeline.clients = clients
	for _, client := range pipeline.clients {
		client.ReassignTo(pipeline)
	}
	pipeline.clientsLock.Unlock()

	// Shut down the old pipelines queue, this will force all the
	//  clients to 'refresh' their consumer, and pick up the new
	//  pipeline queue from the new pipeline.  This will also block
	//  any writers from sending new requests here if they have an
	//  out of date route config.
	oldPipeline.queue.Close()
}

func (pipeline *memdPipeline) GracefulClose() []*memdClient {
	// Shut down all the clients
	pipeline.clientsLock.Lock()
	clients := pipeline.clients
	pipeline.clients = nil
	pipeline.clientsLock.Unlock()

	var memdClients []*memdClient
	for _, pipecli := range clients {
		client := pipecli.CloseAndTakeClient()
		logDebugf("Pipeline %s/%p taking memdclient %p from client %p", pipeline.address, pipeline, client, pipecli)
		if client != nil {
			memdClients = append(memdClients, client)
		}
	}

	// Kill the queue, forcing everyone to stop
	pipeline.queue.Close()

	return memdClients
}

func (pipeline *memdPipeline) Close() error {
	// Shut down all the clients
	pipeline.clientsLock.Lock()
	clients := pipeline.clients
	pipeline.clients = nil
	pipeline.clientsLock.Unlock()

	hadErrors := false
	for _, pipecli := range clients {
		client := pipecli.CloseAndTakeClient()
		if client != nil {

			err := client.Close()
			if err != nil {
				logErrorf("failed to shutdown memdclient: %s", err)
				hadErrors = true
			}

			// Wait for the client to finish closing.
			<-client.CloseNotify()
		}
	}

	// Kill the queue, forcing everyone to stop
	pipeline.queue.Close()

	if hadErrors {
		return errCliInternalError
	}

	return nil
}

func (pipeline *memdPipeline) Drain(cb func(*memdQRequest)) {
	pipeline.queue.Drain(cb)
}
