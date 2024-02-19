package gocbcore

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/mock"
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

func (suite *UnitTestSuite) TestSearchComponentRouteConfigHandling() {
	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.searchQueryComponent"))

	sqc := newSearchQueryComponent(nil, configC, nil)

	suite.Assert().Equal(CapabilityStatusUnknown, sqc.capabilityStatus(SearchCapabilityVectorSearch))
	suite.Assert().Equal(CapabilityStatusUnknown, sqc.capabilityStatus(SearchCapabilityScopedIndexes))

	cfg := &routeConfig{
		clusterCapabilitiesVer: []int{1},
		clusterCapabilities:    map[string][]string{},
	}
	sqc.OnNewRouteConfig(cfg)

	suite.Assert().Equal(CapabilityStatusUnsupported, sqc.capabilityStatus(SearchCapabilityVectorSearch))
	suite.Assert().Equal(CapabilityStatusUnsupported, sqc.capabilityStatus(SearchCapabilityScopedIndexes))

	cfg = &routeConfig{
		clusterCapabilitiesVer: []int{1},
		clusterCapabilities: map[string][]string{
			"search": {"vectorSearch", "scopedSearchIndex"},
		},
	}
	sqc.OnNewRouteConfig(cfg)

	suite.Assert().Equal(CapabilityStatusSupported, sqc.capabilityStatus(SearchCapabilityVectorSearch))
	suite.Assert().Equal(CapabilityStatusSupported, sqc.capabilityStatus(SearchCapabilityScopedIndexes))
}

func (suite *UnitTestSuite) TestSearchComponentVectorSearchUnsupported() {
	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.searchQueryComponent"))

	sqc := newSearchQueryComponent(nil, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))
	sqc.caps[SearchCapabilityVectorSearch] = CapabilityStatusUnsupported
	sqc.caps[SearchCapabilityScopedIndexes] = CapabilityStatusSupported

	opts := SearchQueryOptions{
		BucketName: "test-bucket",
		ScopeName:  "test-scope",
		IndexName:  "test-index",
		Payload:    []byte("{\"knn\":[]}"),
	}
	_, err := sqc.SearchQuery(opts, nil)

	suite.Assert().ErrorIs(err, ErrFeatureNotAvailable)
	suite.Assert().Contains(err.Error(), "vector search is not supported by this cluster version")
}

func (suite *UnitTestSuite) TestSearchComponentScopedIndexUnsupported() {
	configC := new(mockConfigManager)
	configC.On("AddConfigWatcher", mock.AnythingOfType("*gocbcore.searchQueryComponent"))

	sqc := newSearchQueryComponent(nil, configC, newTracerComponent(&noopTracer{}, "", true, &noopMeter{}))
	sqc.caps[SearchCapabilityScopedIndexes] = CapabilityStatusUnsupported

	opts := SearchQueryOptions{
		BucketName: "test-bucket",
		ScopeName:  "test-scope",
		IndexName:  "test-index",
		Payload:    []byte("{}"),
	}
	_, err := sqc.SearchQuery(opts, nil)

	suite.Assert().ErrorIs(err, ErrFeatureNotAvailable)
	suite.Assert().Contains(err.Error(), "scoped search indexes are not supported by this cluster version")
}
