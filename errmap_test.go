package gocbcore

import (
	"github.com/couchbase/gocbcore/v10/memd"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/jcbmock"
)

func TestKvErrorConstantRetry(t *testing.T) {
	constant := kvErrorMapRetry{
		Strategy:    "constant",
		Interval:    1000,
		After:       2000,
		Ceil:        4000,
		MaxDuration: 3000,
	}
	if constant.CalculateRetryDelay(0) != 2000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if constant.CalculateRetryDelay(1) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if constant.CalculateRetryDelay(3) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if constant.CalculateRetryDelay(15000) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for large retry counts")
	}
}

func TestKvErrorLinearRetry(t *testing.T) {
	linear := kvErrorMapRetry{
		Strategy:    "linear",
		Interval:    1000,
		After:       2000,
		Ceil:        60000,
		MaxDuration: 100000,
	}
	if linear.CalculateRetryDelay(0) != 2000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if linear.CalculateRetryDelay(1) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if linear.CalculateRetryDelay(3) != 3000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if linear.CalculateRetryDelay(150) != 60000*time.Millisecond {
		t.Fatalf("should respect ceiling for large retry counts")
	}
}

func TestKvErrorExponentialRetry(t *testing.T) {
	exponential := kvErrorMapRetry{
		Strategy:    "exponential",
		Interval:    10,
		After:       1000,
		Ceil:        60000,
		MaxDuration: 100000,
	}
	if exponential.CalculateRetryDelay(0) != 1000*time.Millisecond {
		t.Fatalf("failed to respect after for first retry")
	}
	if exponential.CalculateRetryDelay(1) != 10*time.Millisecond {
		t.Fatalf("should respect interval for second retry")
	}
	if exponential.CalculateRetryDelay(3) != 1000*time.Millisecond {
		t.Fatalf("should respect interval for minimal retries")
	}
	if exponential.CalculateRetryDelay(400) != 60000*time.Millisecond {
		t.Fatalf("should respect ceiling for large retry counts")
	}
}

type errMapTestRetryStrategy struct {
	reasons []RetryReason
	retries int
}

func (lrs *errMapTestRetryStrategy) RetryAfter(request RetryRequest, reason RetryReason) RetryAction {
	lrs.retries++
	lrs.reasons = append(lrs.reasons, reason)
	return &WithDurationRetryAction{50 * time.Millisecond}
}

func (suite *StandardTestSuite) testKvErrorMapGeneric(errCode uint16) {
	suite.EnsureSupportsFeature(TestFeatureErrMap)

	agent, h := suite.GetAgentAndHarness()
	if !suite.IsMockServer() {
		suite.T().Skipf("only supported when testing against mock server")
	}

	testKey := "hello"

	agent.pollerController.Pause(true)
	defer func() {
		agent.pollerController.Pause(false)
	}()

	suite.mockInst.Control(jcbmock.NewCommand(jcbmock.COpFail, map[string]interface{}{
		"bucket": suite.BucketName,
		"code":   errCode,
		"count":  3,
	}))

	strategy := &errMapTestRetryStrategy{}
	h.PushOp(agent.Get(GetOptions{
		Key:           []byte(testKey),
		RetryStrategy: strategy,
	}, func(res *GetResult, err error) {
		h.Wrap(func() {})
	}))
	h.Wait(0)

	if strategy.retries != 3 {
		suite.T().Fatalf("Expected retries to be 3 but was %d", strategy.retries)
	}

	if len(strategy.reasons) != 3 {
		suite.T().Fatalf("Expected 3 retry reasons but was %v", strategy.reasons)
	}

	for _, reason := range strategy.reasons {
		if reason != KVErrMapRetryReason {
			suite.T().Fatalf("Expected reason to be KVErrMapRetryReason but was %s", reason.Description())
		}
	}

	suite.VerifyKVMetrics("Get", 1, false, false)

	suite.mockInst.Control(jcbmock.NewCommand(jcbmock.COpFail, map[string]interface{}{
		"bucket": suite.BucketName,
		"code":   errCode,
		"count":  0,
	}))
}

// It doesn't actually matter what strategy the error map specifies, we just test that retries happen as the
// strategy dictates no matter what.
func (suite *StandardTestSuite) TestKvErrorMap7ff0() {
	suite.testKvErrorMapGeneric(0x7ff0)
}

func (suite *StandardTestSuite) TestKvErrorMap7ff1() {
	suite.testKvErrorMapGeneric(0x7ff1)
}

func (suite *StandardTestSuite) TestKvErrorMap7ff2() {
	suite.testKvErrorMapGeneric(0x7ff2)
}

func (suite *UnitTestSuite) TestStoreKVErrorMapV1() {
	data, err := loadRawTestDataset("err_map70_v1")
	suite.Require().Nil(err, err)

	errMgr := newErrMapManager("test")
	errMgr.StoreErrorMap(data)

	errMap := errMgr.kvErrorMap.Get()
	suite.Require().NotNil(errMap)
	suite.Assert().Equal(1, errMap.Version)
	suite.Assert().Equal(2, errMap.Revision)
	suite.Assert().Len(errMap.Errors, 58)
	entry := errMgr.getKvErrMapData(memd.StatusLocked)
	suite.Require().NotNil(entry)
	suite.Assert().Equal("LOCKED", entry.Name)
	suite.Assert().Equal("Requested resource is locked", entry.Description)
	suite.Assert().Len(entry.Attributes, 3)
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("item-locked"))
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("item-only"))
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("retry-now"))
}

func (suite *UnitTestSuite) TestStoreKVErrorMapV2() {
	data, err := loadRawTestDataset("err_map71_v2")
	suite.Require().Nil(err, err)

	errMgr := newErrMapManager("test")
	errMgr.StoreErrorMap(data)

	errMap := errMgr.kvErrorMap.Get()
	suite.Require().NotNil(errMap)
	suite.Assert().Equal(2, errMap.Version)
	suite.Assert().Equal(1, errMap.Revision)
	suite.Assert().Len(errMap.Errors, 65)
	entry := errMgr.getKvErrMapData(memd.StatusLocked)
	suite.Require().NotNil(entry)
	suite.Assert().Equal("LOCKED", entry.Name)
	suite.Assert().Equal("Requested resource is locked", entry.Description)
	suite.Assert().Len(entry.Attributes, 3)
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("item-locked"))
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("item-only"))
	suite.Assert().Contains(entry.Attributes, kvErrorMapAttribute("retry-now"))
}
