package gocbcore

type TestName string

const (
	TestNameMemcachedBasic         TestName = "kv/memcached/MemcachedBasic"
	TestNameErrMapLinearRetry      TestName = "kv/errmap/ErrMapLinearRetry"
	TestNameErrMapConstantRetry    TestName = "kv/errmap/ErrMapConstantRetry"
	TestNameErrMapExponentialRetry TestName = "kv/errmap/ErrMapExponentialRetry"
	TestNameExtendedError          TestName = "kv/error/ExtendedError"
)
