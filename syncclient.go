package gocbcore

import (
	"encoding/binary"
	"strings"
	"time"
)

type memdSenderClient interface {
	Address() string
	SendRequest(*memdQRequest) error
}

type syncClient struct {
	client memdSenderClient
}

func (client *syncClient) Address() string {
	return client.client.Address()
}

func (client *syncClient) doRequest(req *memdPacket, deadline time.Time) (respOut *memdPacket, errOut error) {
	signal := make(chan bool)

	qreq := memdQRequest{
		memdPacket: *req,
		Callback: func(resp *memdQResponse, _ *memdQRequest, err error) {
			if resp != nil {
				respOut = &resp.memdPacket
			}
			errOut = err
			signal <- true
		},
	}

	err := client.client.SendRequest(&qreq)
	if err != nil {
		return nil, err
	}

	timeoutTmr := AcquireTimer(deadline.Sub(time.Now()))
	select {
	case <-signal:
		ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		ReleaseTimer(timeoutTmr, true)
		if !qreq.Cancel() {
			<-signal
			return
		}
		return nil, ErrTimeout
	}
}

func (client *syncClient) doBasicOp(cmd commandCode, k, v, e []byte, deadline time.Time) ([]byte, error) {
	resp, err := client.doRequest(&memdPacket{
		Magic:  reqMagic,
		Opcode: cmd,
		Key:    k,
		Value:  v,
		Extras: e,
	}, deadline)

	// We do it this way as the response value could still be useful even if an
	// error status code is returned.  For instance, statusAuthContinue still
	// contains authentication stepping information.
	if resp == nil {
		return nil, err
	}

	return resp.Value, err
}

func (client *syncClient) ExecHello(features []helloFeature, deadline time.Time) error {
	appendFeatureCode := func(bytes []byte, feature helloFeature) []byte {
		bytes = append(bytes, 0, 0)
		binary.BigEndian.PutUint16(bytes[len(bytes)-2:], uint16(feature))
		return bytes
	}

	var featureBytes []byte
	for _, feature := range features {
		featureBytes = appendFeatureCode(featureBytes, feature)
	}

	clientId := []byte("gocb/" + goCbCoreVersionStr)

	_, err := client.doBasicOp(cmdHello, clientId, featureBytes, nil, deadline)
	return err
}

func (client *syncClient) ExecCccpRequest(deadline time.Time) ([]byte, error) {
	return client.doBasicOp(cmdGetClusterConfig, nil, nil, nil, deadline)
}

func (client *syncClient) ExecOpenDcpConsumer(streamName string, deadline time.Time) error {
	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 1)
	_, err := client.doBasicOp(cmdDcpOpenConnection, []byte(streamName), nil, extraBuf, deadline)
	return err
}

func (client *syncClient) ExecSaslListMechs(deadline time.Time) ([]string, error) {
	bytes, err := client.doBasicOp(cmdSASLListMechs, nil, nil, nil, deadline)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(bytes), " "), nil
}

func (client *syncClient) ExecSaslAuth(k, v []byte, deadline time.Time) ([]byte, error) {
	return client.doBasicOp(cmdSASLAuth, k, v, nil, deadline)
}

func (client *syncClient) ExecSaslStep(k, v []byte, deadline time.Time) ([]byte, error) {
	return client.doBasicOp(cmdSASLStep, k, v, nil, deadline)
}

func (client *syncClient) ExecSelectBucket(b []byte, deadline time.Time) error {
	_, err := client.doBasicOp(cmdSelectBucket, nil, b, nil, deadline)
	return err
}
