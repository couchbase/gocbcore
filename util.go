package gocbcore

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
)

func getMapValueString(dict map[string]interface{}, key string, def string) string {
	if dict != nil {
		if val, ok := dict[key]; ok {
			if valStr, ok := val.(string); ok {
				return valStr
			}
		}
	}
	return def
}

func getMapValueBool(dict map[string]interface{}, key string, def bool) bool {
	if dict != nil {
		if val, ok := dict[key]; ok {
			if valStr, ok := val.(bool); ok {
				return valStr
			}
		}
	}
	return def
}

func randomCbUID() []byte {
	out := make([]byte, 8)
	_, err := rand.Read(out)
	if err != nil {
		logWarnf("Crypto read failed: %s", err)
	}
	return out
}

func formatCbUID(data []byte) string {
	return fmt.Sprintf("%02x%02x%02x%02x%02x%02x%02x%02x",
		data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7])
}

func agentName(userAgent string) string {
	agentName := "gocbcore/" + goCbCoreVersionStr
	if userAgent != "" {
		agentName += " " + userAgent
	}
	return agentName
}

// rfcUserAgent builds a user agent following SDK-RFC 47 (unified user agent):
// "<sdk-identifier>/<version> (<system-information>)".
//
// When a wrapping SDK provides its identifier, it leads and gocbcore is listed in the system information,
// e.g. "gocb-columnar/1.0.0 (gocbcore/10.10.0; linux/amd64; go1.26.0)".
func rfcUserAgent(wrapperIdentifier string) string {
	coreIdentifier := "gocbcore/" + strings.TrimPrefix(goCbCoreVersionStr, "v")
	systemInfo := runtime.GOOS + "/" + runtime.GOARCH + "; " + runtime.Version()
	if wrapperIdentifier == "" {
		return coreIdentifier + " (" + systemInfo + ")"
	}

	return wrapperIdentifier + " (" + coreIdentifier + "; " + systemInfo + ")"
}

func clientInfoString(connID, userAgent string) string {
	clientInfo := struct {
		Agent  string `json:"a"`
		ConnID string `json:"i"`
	}{
		Agent:  agentName(userAgent),
		ConnID: connID,
	}
	clientInfoBytes, err := json.Marshal(clientInfo)
	if err != nil {
		logDebugf("Failed to generate client info string: %s", err)
	}

	return string(clientInfoBytes)
}

func trimSchemePrefix(address string) string {
	idx := strings.Index(address, "://")
	if idx < 0 {
		return address
	}

	return address[idx+len("://"):]
}

func revIsNewer(newRev, newEpoch, oldRev, oldEpoch int64) bool {
	if newEpoch > oldEpoch {
		return true
	}

	if newEpoch < oldEpoch {
		return false
	}

	if newRev > oldRev {
		return true
	}

	return false
}
