package gocbcore

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
)

// TestSearchComponentNilRows tests the case where the server returns a rows field but it's set to a null value.
func (suite *UnitTestSuite) TestSearchComponentNilRows() {
	d, err := suite.LoadRawTestDataset("search_hits_nil")
	suite.Require().Nil(err)

	qStreamer, err := newQueryStreamer(ioutil.NopCloser(bytes.NewBuffer(d)), "hits")
	suite.Require().Nil(err, err)

	reader := SearchRowReader{
		streamer: qStreamer,
	}

	numRows := 0
	for reader.NextRow() != nil {
		numRows++
	}
	suite.Assert().Zero(numRows)

	err = reader.Err()
	suite.Require().Nil(err, err)

	metaBytes, err := reader.MetaData()
	suite.Require().Nil(err, err)

	var meta map[string]interface{}
	err = json.Unmarshal(metaBytes, &meta)
	suite.Require().Nil(err, err)

	status := meta["status"].(map[string]interface{})
	errs := status["errors"].(map[string]interface{})

	suite.Assert().Len(errs, 6)
}
