package gocouchbaseio

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type configStreamBlock struct {
	Bytes []byte
}

func (i *configStreamBlock) UnmarshalJSON(data []byte) error {
	i.Bytes = make([]byte, len(data))
	copy(i.Bytes, data)
	return nil
}

func (c *Agent) httpConfigStream(address, hostname, bucket string) {
	uri := fmt.Sprintf("%s/pools/default/bucketsStreaming/%s", address, bucket)
	resp, err := c.httpCli.Get(uri)
	if err != nil {
		return
	}

	dec := json.NewDecoder(resp.Body)
	configBlock := new(configStreamBlock)
	for {
		err := dec.Decode(configBlock)
		if err != nil {
			resp.Body.Close()
			return
		}

		bkCfg, err := parseConfig(configBlock.Bytes, hostname)
		if err == nil {
			c.updateConfig(bkCfg)
		}
	}
}

func hostnameFromUri(uri string) string {
	uriInfo, err := url.Parse(uri)
	if err != nil {
		panic("Failed to parse URI to hostname!")
	}
	return strings.Split(uriInfo.Host, ":")[0]
}

func (c *Agent) httpLooper(firstCfgFn func(*cfgBucket, error)) {
	waitPeriod := 20 * time.Second
	maxConnPeriod := 10 * time.Second
	var iterNum uint64 = 1
	iterSawConfig := false
	seenNodes := make(map[string]uint64)
	isFirstTry := true
	for {
		routingInfo := c.routingInfo.get()

		var pickedSrv string
		for _, srv := range routingInfo.mgmtEpList {
			if seenNodes[srv] >= iterNum {
				continue
			}
			pickedSrv = srv
			break
		}

		fmt.Printf("Http Picked: %s\n", pickedSrv)

		if pickedSrv == "" {
			// All servers have been visited during this iteration
			if isFirstTry {
				fmt.Printf("Pick Failed\n")
				firstCfgFn(nil, &agentError{"Failed to connect to all specified hosts."})
				return
			} else {
				if !iterSawConfig {
					fmt.Printf("Looper waiting...\n")
					// Wait for a period before trying again if there was a problem...
					<-time.After(waitPeriod)
				}
				fmt.Printf("Looping again\n")
				// Go to next iteration and try all servers again
				iterNum++
				iterSawConfig = false
				continue
			}
		}

		hostname := hostnameFromUri(pickedSrv)

		fmt.Printf("HTTP Hostname: %s\n", pickedSrv)

		// HTTP request time!
		uri := fmt.Sprintf("%s/pools/default/bucketsStreaming/%s", pickedSrv, c.bucket)
		resp, err := c.httpCli.Get(uri)
		if err != nil {
			return
		}

		fmt.Printf("Connected\n")

		// Autodisconnect eventually
		go func() {
			<-time.After(maxConnPeriod)
			fmt.Printf("Auto DC!\n")
			resp.Body.Close()
		}()

		dec := json.NewDecoder(resp.Body)
		configBlock := new(configStreamBlock)
		for {
			err := dec.Decode(configBlock)
			if err != nil {
				resp.Body.Close()
				break
			}

			fmt.Printf("Got Block.\n")

			bkCfg, err := parseConfig(configBlock.Bytes, hostname)
			if err != nil {
				resp.Body.Close()
				break
			}

			fmt.Printf("Got Config\n")

			iterSawConfig = true
			if isFirstTry {
				fmt.Printf("HTTP Config Init\n")
				firstCfgFn(bkCfg, nil)
				isFirstTry = false
			} else {
				fmt.Printf("HTTP Config Update\n")
				c.updateConfig(bkCfg)
			}
		}

		fmt.Printf("HTTP, Setting %s to iter %d\n", pickedSrv, iterNum)
		seenNodes[pickedSrv] = iterNum
	}
}
