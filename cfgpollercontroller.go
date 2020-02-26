package gocbcore

type configPollerController interface {
	Pause(paused bool)
	Done() chan struct{}
	Stop()
}
