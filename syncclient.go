package gocbcore

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

type memdSenderClient interface {
	SupportsFeature(HelloFeature) bool
	Address() string
	SendRequest(*memdQRequest) error
}

type memdPipelineSenderWrap struct {
	pipeline *memdPipeline
}

func (wrap *memdPipelineSenderWrap) SupportsFeature(feature HelloFeature) bool {
	return false
}

func (wrap *memdPipelineSenderWrap) Address() string {
	return wrap.pipeline.Address()
}

func (wrap *memdPipelineSenderWrap) SendRequest(req *memdQRequest) error {
	return wrap.pipeline.SendRequest(req)
}

type syncClient struct {
	client memdSenderClient
}

func (client *syncClient) SupportsFeature(feature HelloFeature) bool {
	return client.client.SupportsFeature(feature)
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
	// error status code is returned.  For instance, StatusAuthContinue still
	// contains authentication stepping information.
	if resp == nil {
		return nil, err
	}

	return resp.Value, err
}

// BytesAndError contains the raw bytes of the result of an operation, and/or the error that occurred.
type BytesAndError struct {
	Err   error
	Bytes []byte
}

func (client *syncClient) doAsyncRequest(req *memdPacket, deadline time.Time) (chan BytesAndError, error) {
	completedCh := make(chan BytesAndError, 1)

	signal := make(chan BytesAndError)
	qreq := memdQRequest{
		memdPacket: *req,
		Callback: func(resp *memdQResponse, _ *memdQRequest, err error) {
			signalResp := BytesAndError{}
			if resp != nil {
				signalResp.Bytes = resp.memdPacket.Value
			}
			signalResp.Err = err
			signal <- signalResp
		},
	}

	err := client.client.SendRequest(&qreq)
	if err != nil {
		return nil, err
	}

	timeoutTmr := AcquireTimer(deadline.Sub(time.Now()))
	go func() {
		select {
		case resp := <-signal:
			ReleaseTimer(timeoutTmr, false)
			completedCh <- resp
			return
		case <-timeoutTmr.C:
			ReleaseTimer(timeoutTmr, true)
			if !qreq.Cancel() {
				resp := <-signal
				completedCh <- resp
				return
			}
			completedCh <- BytesAndError{Err: ErrTimeout}
		}
	}()

	return completedCh, nil
}

func (client *syncClient) doAsyncOp(cmd commandCode, k, v, e []byte, deadline time.Time) (chan BytesAndError, error) {
	return client.doAsyncRequest(&memdPacket{
		Magic:  reqMagic,
		Opcode: cmd,
		Key:    k,
		Value:  v,
		Extras: e,
	}, deadline)
}

func (client *syncClient) ExecDcpControl(key string, value string, deadline time.Time) error {
	_, err := client.doBasicOp(cmdDcpControl, []byte(key), []byte(value), nil, deadline)
	return err
}

// ExecHelloResponse contains the features and/or error from an ExecHello operation.
type ExecHelloResponse struct {
	SrvFeatures []HelloFeature
	Err         error
}

func (client *syncClient) ExecHello(clientId string, features []HelloFeature, deadline time.Time) (chan ExecHelloResponse, error) {
	appendFeatureCode := func(bytes []byte, feature HelloFeature) []byte {
		bytes = append(bytes, 0, 0)
		binary.BigEndian.PutUint16(bytes[len(bytes)-2:], uint16(feature))
		return bytes
	}

	var featureBytes []byte
	for _, feature := range features {
		featureBytes = appendFeatureCode(featureBytes, feature)
	}

	completedCh := make(chan ExecHelloResponse)
	opCh, err := client.doAsyncOp(cmdHello, []byte(clientId), featureBytes, nil, deadline)
	if err != nil {
		return nil, err
	}

	go func() {
		resp := <-opCh
		if resp.Err != nil {
			completedCh <- ExecHelloResponse{
				Err: resp.Err,
			}
			return
		}

		var srvFeatures []HelloFeature
		for i := 0; i < len(resp.Bytes); i += 2 {
			feature := binary.BigEndian.Uint16(resp.Bytes[i:])
			srvFeatures = append(srvFeatures, HelloFeature(feature))
		}

		completedCh <- ExecHelloResponse{
			SrvFeatures: srvFeatures,
		}
	}()

	return completedCh, nil
}

func (client *syncClient) ExecGetClusterConfig(deadline time.Time) ([]byte, error) {
	return client.doBasicOp(cmdGetClusterConfig, nil, nil, nil, deadline)
}

func (client *syncClient) ExecGetErrorMap(version uint16, deadline time.Time) (chan BytesAndError, error) {
	valueBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(valueBuf, version)
	return client.doAsyncOp(cmdGetErrorMap, nil, valueBuf, nil, deadline)
}

func (client *syncClient) ExecOpenDcpConsumer(streamName string, openFlags DcpOpenFlag, deadline time.Time) error {
	_, ok := client.client.(*memdClient)
	if !ok {
		return ErrCliInternalError
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], uint32((openFlags & ^DcpOpenFlag(3))|DcpOpenFlagProducer))
	_, err := client.doBasicOp(cmdDcpOpenConnection, []byte(streamName), nil, extraBuf, deadline)
	return err
}

func (client *syncClient) ExecEnableDcpNoop(period time.Duration, deadline time.Time) error {
	_, ok := client.client.(*memdClient)
	if !ok {
		return ErrCliInternalError
	}
	// The client will always reply to No-Op's.  No need to enable it

	err := client.ExecDcpControl("enable_noop", "true", deadline)
	if err != nil {
		return err
	}

	periodStr := fmt.Sprintf("%d", period/time.Second)
	err = client.ExecDcpControl("set_noop_interval", periodStr, deadline)
	if err != nil {
		return err
	}

	return nil
}

func (client *syncClient) ExecEnableDcpClientEnd(deadline time.Time) error {
	memcli, ok := client.client.(*memdClient)
	if !ok {
		return ErrCliInternalError
	}

	err := client.ExecDcpControl("send_stream_end_on_client_close_stream", "true", deadline)
	if err != nil {
		memcli.streamEndNotSupported = true
	}

	return nil
}

func (client *syncClient) ExecEnableDcpBufferAck(bufferSize int, deadline time.Time) error {
	mclient, ok := client.client.(*memdClient)
	if !ok {
		return ErrCliInternalError
	}

	// Enable buffer acknowledgment on the client
	mclient.EnableDcpBufferAck(bufferSize / 2)

	bufferSizeStr := fmt.Sprintf("%d", bufferSize)
	err := client.ExecDcpControl("connection_buffer_size", bufferSizeStr, deadline)
	if err != nil {
		return err
	}

	return nil
}

func (client *syncClient) SaslListMechs(deadline time.Time) (chan SaslListMechsCompleted, error) {
	completedCh := make(chan SaslListMechsCompleted)

	opCh, err := client.doAsyncOp(cmdSASLListMechs, nil, nil, nil, deadline)
	if err != nil {
		return nil, err
	}

	go func() {
		resp := <-opCh
		if resp.Err != nil {
			completedCh <- SaslListMechsCompleted{
				Err: resp.Err,
			}
			return
		}

		mechs := strings.Split(string(resp.Bytes), " ")
		var authMechs []AuthMechanism
		for _, mech := range mechs {
			authMechs = append(authMechs, AuthMechanism(mech))
		}

		completedCh <- SaslListMechsCompleted{
			Mechs: authMechs,
		}
	}()

	return completedCh, nil
}

func (client *syncClient) SaslAuth(k, v []byte, deadline time.Time) (chan BytesAndError, error) {
	return client.doAsyncOp(cmdSASLAuth, k, v, nil, deadline)
}

func (client *syncClient) SaslStep(k, v []byte, deadline time.Time) (chan BytesAndError, error) {
	return client.doAsyncOp(cmdSASLStep, k, v, nil, deadline)
}

func (client *syncClient) ExecSelectBucket(b []byte, deadline time.Time) (chan BytesAndError, error) {
	return client.doAsyncOp(cmdSelectBucket, b, nil, nil, deadline)
}
