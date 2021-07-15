package gocbcore

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocbcore/v10/memd"
	"time"
)

func (suite *UnitTestSuite) TestZombieLoggerComponent() {
	responses := []*memdQResponse{
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  23,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2100 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  24,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2200 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  25,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 1100 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdGet,
				Opaque:  27,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 2800 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
		{
			Packet: &memd.Packet{
				Command: memd.CmdReplace,
				Opaque:  29,
				ServerDurationFrame: &memd.ServerDurationFrame{
					ServerDuration: 5000 * time.Microsecond,
				},
			},
			sourceAddr:   "10.112.210.101",
			sourceConnID: "9a1e99041b33322b/54cf79f08d852738",
		},
	}

	z := newZombieLoggerComponent(1*time.Second, 4)
	go z.Start()
	for _, r := range responses {
		z.RecordZombieResponse(r, "9a1e99041b33322b/54cf79f08d852738", "10.112.210.1", "10.112.210.101")
	}
	z.Stop()

	jsonOutput := z.createOutput()

	type expectedOutputFormat struct {
		ConnectionID     string `json:"last_local_id"`
		OperationID      string `json:"operation_id"`
		RemoteSocket     string `json:"last_remote_socket,omitempty"`
		LocalSocket      string `json:"last_local_socket,omitempty"`
		ServerDurationUs uint64 `json:"last_server_duration_us,omitempty"`
		OperationName    string `json:"operation_name"`
	}

	expectedOutput := []expectedOutputFormat{
		{
			ConnectionID:     "9a1e99041b33322b/54cf79f08d852738",
			OperationID:      "0x1d",
			RemoteSocket:     "10.112.210.101",
			LocalSocket:      "10.112.210.1",
			ServerDurationUs: 5000,
			OperationName:    memd.CmdReplace.Name(),
		},
		{
			ConnectionID:     "9a1e99041b33322b/54cf79f08d852738",
			OperationID:      "0x1b",
			RemoteSocket:     "10.112.210.101",
			LocalSocket:      "10.112.210.1",
			ServerDurationUs: 2800,
			OperationName:    memd.CmdGet.Name(),
		},
		{
			ConnectionID:     "9a1e99041b33322b/54cf79f08d852738",
			OperationID:      "0x18",
			RemoteSocket:     "10.112.210.101",
			LocalSocket:      "10.112.210.1",
			ServerDurationUs: 2200,
			OperationName:    memd.CmdReplace.Name(),
		},
		{
			ConnectionID:     "9a1e99041b33322b/54cf79f08d852738",
			OperationID:      "0x17",
			RemoteSocket:     "10.112.210.101",
			LocalSocket:      "10.112.210.1",
			ServerDurationUs: 2100,
			OperationName:    memd.CmdReplace.Name(),
		},
	}

	expectedJsonOutput, err := json.Marshal(expectedOutput)
	suite.Require().Nil(err)

	var mapTopOutput map[string]json.RawMessage
	suite.Require().Nil(json.Unmarshal(jsonOutput, &mapTopOutput))

	suite.Require().Contains(mapTopOutput, "kv")

	var mapInnerOutput map[string]json.RawMessage
	suite.Require().Nil(json.Unmarshal(mapTopOutput["kv"], &mapInnerOutput))

	suite.Require().Contains(mapInnerOutput, "total_count")
	suite.Require().Contains(mapInnerOutput, "top_requests")

	var totalCount int
	suite.Require().Nil(json.Unmarshal(mapInnerOutput["total_count"], &totalCount))
	suite.Assert().Equal(4, totalCount)

	suite.Assert().Equal(expectedJsonOutput, []byte(mapInnerOutput["top_requests"]), fmt.Sprintf("Expected output to be %s but was %s", string(expectedJsonOutput), string(mapInnerOutput["top_requests"])))
}
