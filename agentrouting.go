package gocbcore

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"time"
)

type memdInitFunc func(*syncClient, time.Time, *Agent) error

func checkSupportsFeature(srvFeatures []HelloFeature, feature HelloFeature) bool {
	for _, srvFeature := range srvFeatures {
		if srvFeature == feature {
			return true
		}
	}
	return false
}

func (agent *Agent) dialMemdClient(address string, deadline time.Time) (*memdClient, error) {
	// Copy the tls configuration since we need to provide the hostname for each
	// server that we connect to so that the certificate can be validated properly.
	var tlsConfig *tls.Config
	if agent.tlsConfig != nil {
		host, err := hostFromHostPort(address)
		if err != nil {
			logErrorf("Failed to parse address for TLS config (%s)", err)
		}

		tlsConfig = cloneTLSConfig(agent.tlsConfig)
		tlsConfig.ServerName = host
	}

	conn, err := dialMemdConn(address, tlsConfig, deadline)
	if err != nil {
		logDebugf("Failed to connect. %v", err)
		return nil, err
	}

	client := newMemdClient(agent, conn, agent.circuitBreakerConfig)

	return client, err
}

func continueAfterAuth(sclient *syncClient, bucketName string, continueAuthCh chan bool, deadline time.Time) chan BytesAndError {
	if bucketName == "" {
		return nil
	}

	selectCh := make(chan BytesAndError, 1)
	go func() {
		success := <-continueAuthCh
		if !success {
			selectCh <- BytesAndError{}
			return
		}
		execCh, err := sclient.ExecSelectBucket([]byte(bucketName), deadline)
		if err != nil {
			logDebugf("Failed to execute select bucket (%v)", err)
			selectCh <- BytesAndError{Err: err}
			return
		}

		execResp := <-execCh
		selectCh <- execResp
	}()

	return selectCh
}

func findNextAuthMechanism(authMechanisms []AuthMechanism, serverAuthMechanisms []AuthMechanism) (bool, AuthMechanism, []AuthMechanism) {
	for {
		if len(authMechanisms) <= 1 {
			break
		}
		authMechanisms = authMechanisms[1:]
		mech := authMechanisms[0]
		for _, serverMech := range serverAuthMechanisms {
			if mech == serverMech {
				return true, mech, authMechanisms
			}
		}
	}

	return false, "", authMechanisms
}

func (agent *Agent) storeErrorMap(mapBytes []byte, client *memdClient) {
	errMap, err := parseKvErrorMap(mapBytes)
	if err != nil {
		logDebugf("Failed to parse kv error map (%s)", err)
		return
	}

	logDebugf("Fetched error map: %+v", errMap)

	// Tell the local client to use this error map
	client.SetErrorMap(errMap)

	// Check if we need to switch the agent itself to a better
	//  error map revision.
	for {
		origMap := agent.kvErrorMap.Get()
		if origMap != nil && errMap.Revision < origMap.Revision {
			break
		}

		if agent.kvErrorMap.Update(origMap, errMap) {
			break
		}
	}
}

func (agent *Agent) bootstrap(client *memdClient, deadline time.Time) error {
	sclient := syncClient{
		client: client,
	}

	logDebugf("Fetching cluster client data")

	bucket := agent.bucket()
	features := agent.helloFeatures()
	clientInfoStr := clientInfoString(client.connID, agent.userAgent)
	authMechanisms := agent.authMechanisms

	helloCh, err := sclient.ExecHello(clientInfoStr, features, deadline)
	if err != nil {
		logDebugf("Failed to execute HELLO (%v)", err)
		return err
	}

	errMapCh, err := sclient.ExecGetErrorMap(1, deadline)
	if err != nil {
		// GetErrorMap isn't integral to bootstrap succeeding
		logDebugf("Failed to execute Get error map (%v)", err)
	}

	var listMechsCh chan SaslListMechsCompleted
	firstAuthMethod := agent.authHandler(&sclient, deadline, authMechanisms[0])
	// If the auth method is nil then we don't actually need to do any auth so no need to Get the mechanisms.
	if firstAuthMethod != nil {
		listMechsCh = make(chan SaslListMechsCompleted)
		err = sclient.SaslListMechs(deadline, func(mechs []AuthMechanism, err error) {
			if err != nil {
				logDebugf("Failed to fetch list auth mechs (%v)", err)
			}
			listMechsCh <- SaslListMechsCompleted{
				Err:   err,
				Mechs: mechs,
			}
		})
		if err != nil {
			logDebugf("Failed to execute list auth mechs (%v)", err)
		}
	}

	var completedAuthCh chan BytesAndError
	var continueAuthCh chan bool
	if firstAuthMethod != nil {
		completedAuthCh, continueAuthCh, err = firstAuthMethod()
		if err != nil {
			logDebugf("Failed to execute auth (%v)", err)
			return err
		}
	}

	var selectCh chan BytesAndError
	if continueAuthCh == nil {
		if bucket != "" {
			selectCh, err = sclient.ExecSelectBucket([]byte(bucket), deadline)
			if err != nil {
				logDebugf("Failed to execute select bucket (%v)", err)
				return err
			}
		}
	} else {
		selectCh = continueAfterAuth(&sclient, bucket, continueAuthCh, deadline)
	}

	helloResp := <-helloCh
	if helloResp.Err != nil {
		logDebugf("Failed to hello with server (%v)", helloResp.Err)
		return helloResp.Err
	}

	errMapResp := <-errMapCh
	if errMapResp.Err == nil {
		agent.storeErrorMap(errMapResp.Bytes, client)
	} else {
		logDebugf("Failed to fetch kv error map (%s)", errMapResp.Err)
	}

	var serverAuthMechanisms []AuthMechanism
	if listMechsCh != nil {
		listMechsResp := <-listMechsCh
		if listMechsResp.Err == nil {
			serverAuthMechanisms = listMechsResp.Mechs
			logDebugf("Server supported auth mechanisms: %v", serverAuthMechanisms)
		} else {
			logDebugf("Failed to fetch auth mechs from server (%v)", listMechsResp.Err)
		}
	}

	// If completedAuthCh isn't nil then we have attempted to do auth so we need to wait on the result of that.
	if completedAuthCh != nil {
		authResp := <-completedAuthCh
		if authResp.Err != nil {
			logDebugf("Failed to perform auth against server (%v)", authResp.Err)
			// If there's an auth failure or there was only 1 mechanism to use then fail.
			if len(authMechanisms) == 1 || errors.Is(authResp.Err, ErrAuthenticationFailure) {
				return authResp.Err
			}

			for {
				var found bool
				var mech AuthMechanism
				found, mech, authMechanisms = findNextAuthMechanism(authMechanisms, serverAuthMechanisms)
				if !found {
					logDebugf("Failed to authenticate, all options exhausted")
					return authResp.Err
				}

				nextAuthFunc := agent.authHandler(&sclient, deadline, mech)
				if nextAuthFunc == nil {
					// This can't really happen but just in case it somehow does.
					logDebugf("Failed to authenticate, no available credentials")
					return authResp.Err
				}
				completedAuthCh, continueAuthCh, err = nextAuthFunc()
				if err != nil {
					logDebugf("Failed to execute auth (%v)", err)
					return err
				}
				if continueAuthCh == nil {
					if bucket != "" {
						selectCh, err = sclient.ExecSelectBucket([]byte(bucket), deadline)
						if err != nil {
							logDebugf("Failed to execute select bucket (%v)", err)
							return err
						}
					}
				} else {
					selectCh = continueAfterAuth(&sclient, bucket, continueAuthCh, deadline)
				}
				authResp = <-completedAuthCh
				if authResp.Err == nil {
					break
				}

				logDebugf("Failed to perform auth against server (%v)", authResp.Err)
				if errors.Is(authResp.Err, ErrAuthenticationFailure) {
					return authResp.Err
				}
			}
		}
		logDebugf("Authenticated successfully")
	}

	if selectCh != nil {
		selectResp := <-selectCh
		if selectResp.Err != nil {
			logDebugf("Failed to perform select bucket against server (%v)", selectResp.Err)
			return selectResp.Err
		}
	}

	client.features = helloResp.SrvFeatures

	logDebugf("Client Features: %+v", features)
	logDebugf("Server Features: %+v", client.features)

	collectionsSupported := client.SupportsFeature(FeatureCollections)
	if collectionsSupported {
		client.conn.EnableCollections(true)
	}
	agent.updateCollectionsSupport(collectionsSupported)
	agent.updateDurabilitySupport(client.SupportsFeature(FeatureEnhancedDurability))

	if client.SupportsFeature(FeatureDurations) {
		client.conn.EnableFramingExtras(true)
	}

	err = agent.initFn(&sclient, deadline, agent)
	if err != nil {
		return err
	}

	return nil
}

func clientInfoString(connID, userAgent string) string {
	agentName := "gocbcore/" + goCbCoreVersionStr
	if userAgent != "" {
		agentName += " " + userAgent
	}

	clientInfo := struct {
		Agent  string `json:"a"`
		ConnID string `json:"i"`
	}{
		Agent:  agentName,
		ConnID: connID,
	}
	clientInfoBytes, err := json.Marshal(clientInfo)
	if err != nil {
		logDebugf("Failed to generate client info string: %s", err)
	}

	return string(clientInfoBytes)
}

func (agent *Agent) helloFeatures() []HelloFeature {
	var features []HelloFeature

	// Send the TLS flag, which has unknown effects.
	features = append(features, FeatureTLS)

	// Indicate that we understand XATTRs
	features = append(features, FeatureXattr)

	// Indicates that we understand select buckets.
	features = append(features, FeatureSelectBucket)

	// If the user wants to use KV Error maps, lets enable them
	features = append(features, FeatureXerror)

	// If the user wants to use mutation tokens, lets enable them
	if agent.useMutationTokens {
		features = append(features, FeatureSeqNo)
	}

	// If the user wants on-the-wire compression, lets try to enable it
	if agent.useCompression {
		features = append(features, FeatureSnappy)
	}

	if agent.useDurations {
		features = append(features, FeatureDurations)
	}

	if agent.useCollections {
		features = append(features, FeatureCollections)
	}

	// These flags are informational so don't actually enable anything
	// but the enhanced durability flag tells us if the server supports
	// the feature
	features = append(features, FeatureAltRequests)
	features = append(features, FeatureEnhancedDurability)

	return features
}

func (agent *Agent) slowDialMemdClient(address string) (*memdClient, error) {
	agent.serverFailuresLock.Lock()
	failureTime := agent.serverFailures[address]
	agent.serverFailuresLock.Unlock()

	if !failureTime.IsZero() {
		waitedTime := time.Since(failureTime)
		if waitedTime < agent.serverWaitTimeout {
			time.Sleep(agent.serverWaitTimeout - waitedTime)
		}
	}

	deadline := time.Now().Add(agent.kvConnectTimeout)
	client, err := agent.dialMemdClient(address, deadline)
	if err != nil {
		agent.serverFailuresLock.Lock()
		agent.serverFailures[address] = time.Now()
		agent.serverFailuresLock.Unlock()

		return nil, err
	}

	err = agent.bootstrap(client, deadline)
	if err != nil {
		closeErr := client.Close()
		if closeErr != nil {
			logWarnf("Failed to close authentication client (%s)", closeErr)
		}
		agent.serverFailuresLock.Lock()
		agent.serverFailures[address] = time.Now()
		agent.serverFailuresLock.Unlock()

		return nil, err
	}

	return client, nil
}
