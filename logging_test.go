package gocbcore

import (
	"bytes"
	"log"
)

func (suite *UnitTestSuite) TestLogRedaction() {
	var logs bytes.Buffer
	gologger := log.New(&logs, "", 0)
	sdklogger := defaultLogger{
		GoLogger: gologger,
		Level:    LogDebug,
	}

	if suite.Assert().NoError(sdklogger.Log(LogDebug, 1, redactUserData("sensitive user data"))) {
		suite.Assert().Equal("<ud>sensitive user data</ud>\n", logs.String())
	}

	logs.Reset()

	if suite.Assert().NoError(sdklogger.Log(LogDebug, 1, redactMetaData("sensitive meta data"))) {
		suite.Assert().Equal("<md>sensitive meta data</md>\n", logs.String())
	}

	logs.Reset()

	if suite.Assert().NoError(sdklogger.Log(LogDebug, 1, redactSystemData("sensitive system data"))) {
		suite.Assert().Equal("<sd>sensitive system data</sd>\n", logs.String())
	}
}
