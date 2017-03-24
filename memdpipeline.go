package gocbcore

import (
	"errors"
	"fmt"
	"sync"
)

var (
	errPipelineClosed = errors.New("pipeline has been closed")
	errPipelineFull   = errors.New("pipeline is too full")
)

type memdGetClientFn func() (*memdClient, error)

type memdPipeline struct {
	address     string
	getClientFn memdGetClientFn
	maxItems    int
	queue       *memdOpQueue
	maxClients  int
	clients     []*memdPipelineClient
}

func newPipeline(address string, maxClients, maxItems int, getClientFn memdGetClientFn) *memdPipeline {
	return &memdPipeline{
		address:     address,
		getClientFn: getClientFn,
		maxClients:  maxClients,
		maxItems:    maxItems,
		queue:       newMemdOpQueue(),
	}
}

func newDeadPipeline(maxItems int) *memdPipeline {
	return newPipeline("", 0, maxItems, nil)
}

func (pipeline memdPipeline) debugString() string {
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

func (pipeline memdPipeline) Address() string {
	return pipeline.address
}

func (pipeline *memdPipeline) StartClients() {
	for len(pipeline.clients) < pipeline.maxClients {
		client := newMemdPipelineClient(pipeline)
		pipeline.clients = append(pipeline.clients, client)

		go client.Run()
	}
}

func (pipeline *memdPipeline) sendRequest(req *memdQRequest, maxItems int) error {
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

// Performs a takeover of another pipeline.  Note that this does not
//  take over the requests queued in the old pipeline, and those must
//  be drained and processed separately.
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
			req.tryCallback(nil, ErrInternalError)
		})

		return
	}

	// Migrate all the clients to the new pipeline
	pipeline.clients = oldPipeline.clients
	oldPipeline.clients = nil
	for _, client := range pipeline.clients {
		client.ReassignTo(pipeline)
	}

	// Shut down the old pipelines queue, this will force all the
	//  clients to 'refresh' their consumer, and pick up the new
	//  pipeline queue from the new pipeline.
	oldPipeline.queue.Close()
}

func (pipeline *memdPipeline) Close() error {
	// Shut down all the clients
	var errs MultiError
	for _, pipecli := range pipeline.clients {
		err := pipecli.Close()
		if err != nil {
			errs.add(err)
		}
	}

	// Kill the queue, forcing everyone to stop
	pipeline.queue.Close()

	return errs.get()
}

func (pipeline *memdPipeline) Drain(cb func(*memdQRequest)) {
	pipeline.queue.Drain(cb)
}

type memdPipelineClient struct {
	parent     *memdPipeline
	clientDead bool
	consumer   *memdOpConsumer
	lock       sync.Mutex
}

func newMemdPipelineClient(parent *memdPipeline) *memdPipelineClient {
	return &memdPipelineClient{
		parent: parent,
	}
}

func (pipecli *memdPipelineClient) ReassignTo(parent *memdPipeline) {
	pipecli.lock.Lock()
	pipecli.parent = parent
	consumer := pipecli.consumer
	pipecli.lock.Unlock()

	if consumer != nil {
		consumer.Close()
	}
}

func (pipecli *memdPipelineClient) ioLoop(client *memdClient) {
	pipecli.lock.Lock()
	if pipecli.parent == nil {
		logErrorf("Pipeline ioLoop started with no parent")
		pipecli.lock.Unlock()

		err := client.Close()
		if err != nil {
			logErrorf("Failed to shut down broken ioLoop client (%s)", err)
		}

		return
	}
	address := pipecli.parent.address
	pipecli.lock.Unlock()

	killSig := make(chan struct{})

	go func() {
		logDebugf("Pipeline `%s/%p` client watcher starting...", address, pipecli)

		<-client.CloseNotify()

		logDebugf("Pipeline `%s/%p` client died", address, pipecli)

		pipecli.lock.Lock()
		pipecli.clientDead = true
		consumer := pipecli.consumer
		pipecli.lock.Unlock()

		logDebugf("Pipeline `%s/%p` closing consumer %p", address, pipecli, consumer)
		consumer.Close()

		killSig <- struct{}{}
	}()

	logDebugf("Pipeline `%s/%p` IO loop starting...", address, pipecli)

	var localConsumer *memdOpConsumer
	for {
		if localConsumer == nil {
			logDebugf("Pipeline `%s/%p` fetching new consumer", address, pipecli)

			pipecli.lock.Lock()

			if pipecli.parent == nil {
				logDebugf("Pipeline `%s/%p` found no parent", address, pipecli)

				// This pipelineClient has been shut down
				pipecli.lock.Unlock()
				break
			}

			if pipecli.clientDead {
				// The client has disconnected from the server
				pipecli.lock.Unlock()
				break
			}

			localConsumer = pipecli.parent.queue.Consumer()
			pipecli.consumer = localConsumer
			pipecli.lock.Unlock()
		}

		req := localConsumer.Pop()
		if req == nil {
			localConsumer = nil
			continue
		}

		err := client.SendRequest(req)
		if err != nil {
			logDebugf("Server write error: %v", err)

			// We need to alert the caller that there was a network error
			req.Callback(nil, req, ErrNetwork)

			// Stop looping
			break
		}
	}

	// Ensure the connection is fully closed
	err := client.Close()
	if err != nil {
		// We just log here, since this is a non-fatal error.
		logErrorf("Failed to shut down client socket (%s)", err)
	}

	// We must wait for the close wait goroutine to die as well before we can continue.
	<-killSig
}

func (pipecli *memdPipelineClient) Run() {
	for {
		logDebugf("Pipeline Client `%p` preparing for new client loop", pipecli)

		pipecli.lock.Lock()
		pipeline := pipecli.parent
		pipecli.clientDead = false
		pipecli.lock.Unlock()

		logDebugf("Pipeline Client `%p` is on parent: %p", pipecli, pipeline)

		if pipeline == nil {
			break
		}

		logDebugf("Pipeline Client `%p` retrieving new client connection", pipecli)
		client, err := pipeline.getClientFn()
		if err != nil {
			continue
		}

		// Runs until the connection has died (for whatever reason)
		logDebugf("Pipeline Client `%p` starting new client loop", pipecli)
		pipecli.ioLoop(client)
	}
}

func (pipecli *memdPipelineClient) Close() error {
	logDebugf("Pipeline Client `%p` received close request", pipecli)

	pipecli.lock.Lock()
	pipecli.parent = nil
	consumer := pipecli.consumer
	pipecli.lock.Unlock()

	if consumer != nil {
		consumer.Close()
	}

	return nil
}
