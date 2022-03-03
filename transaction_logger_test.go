package gocbcore

import (
	"fmt"
	"github.com/google/uuid"
)

func (suite *UnitTestSuite) TestTransactionLogger() {
	txnID := uuid.NewString()
	baseLogger := NewInMemoryTransactionLogger()
	logger := newInternalTransactionLogger(txnID, baseLogger)

	itemToLog := struct {
		SomeItem string
	}{"someitem"}

	id1 := uuid.NewString()
	id2 := uuid.NewString()
	id3 := uuid.NewString()
	id4 := uuid.NewString()
	logger.logDebugf(id1, "encountered issue: %s", "error occurred")
	logger.logInfof(id2, "sending request: %d", 112)
	logger.logSchedf(id3, "sending request: %v", itemToLog)
	logger.logInfof(id4, "%s %d %v", "error occurred", 122, itemToLog)

	logs := baseLogger.Logs()

	suite.Require().Len(logs, 4)
	// Timestamp is 12 chars, plus a space.
	suite.Assert().Equal(fmt.Sprintf("%s encountered issue: error occurred", loggerIDShort(txnID, id1)), logs[0].String()[13:])
	suite.Assert().Equal(LogDebug, logs[0].Level)
	suite.Assert().Equal(fmt.Sprintf("%s sending request: 112", loggerIDShort(txnID, id2)), logs[1].String()[13:])
	suite.Assert().Equal(LogInfo, logs[1].Level)
	suite.Assert().Equal(fmt.Sprintf("%s sending request: {someitem}", loggerIDShort(txnID, id3)), logs[2].String()[13:])
	suite.Assert().Equal(LogSched, logs[2].Level)
	suite.Assert().Equal(fmt.Sprintf("%s error occurred 122 {someitem}", loggerIDShort(txnID, id4)), logs[3].String()[13:])
	suite.Assert().Equal(LogInfo, logs[3].Level)
}

func (suite *UnitTestSuite) TestTransactionLoggerRedact() {
	redacted := newLoggableDocKey("bucket", "scope", "collection", []byte("docID"))
	suite.Assert().Equal("bucket.scope.collection.docID", redacted.String())
	redacted = newLoggableDocKey("bucket", "", "collection", []byte("docID"))
	suite.Assert().Equal("bucket._default.collection.docID", redacted.String())
	redacted = newLoggableDocKey("bucket", "scope", "", []byte("docID"))
	suite.Assert().Equal("bucket.scope._default.docID", redacted.String())
	redacted = newLoggableDocKey("bucket", "", "", []byte("docID"))
	suite.Assert().Equal("bucket._default._default.docID", redacted.String())
	SetLogRedactionLevel(RedactPartial)
	redacted = newLoggableDocKey("bucket", "scope", "collection", []byte("docID"))
	suite.Assert().Equal("<ud>bucket.scope.collection.docID</ud>", redacted.String())
	redacted = newLoggableDocKey("bucket", "", "collection", []byte("docID"))
	suite.Assert().Equal("<ud>bucket._default.collection.docID</ud>", redacted.String())
	redacted = newLoggableDocKey("bucket", "scope", "", []byte("docID"))
	suite.Assert().Equal("<ud>bucket.scope._default.docID</ud>", redacted.String())
	redacted = newLoggableDocKey("bucket", "", "", []byte("docID"))
	suite.Assert().Equal("<ud>bucket._default._default.docID</ud>", redacted.String())
	SetLogRedactionLevel(RedactNone)
}
func (suite *UnitTestSuite) TestTransactionLoggerRedactATR() {
	redacted := newLoggableATRKey("bucket", "scope", "collection", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("bucket.scope.collection._txn:atr-0-#14", redacted.String())
	redacted = newLoggableATRKey("bucket", "", "collection", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("bucket._default.collection._txn:atr-0-#14", redacted.String())
	redacted = newLoggableATRKey("bucket", "scope", "", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("bucket.scope._default._txn:atr-0-#14", redacted.String())
	redacted = newLoggableATRKey("bucket", "", "", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("bucket._default._default._txn:atr-0-#14", redacted.String())
	SetLogRedactionLevel(RedactFull)
	redacted = newLoggableATRKey("bucket", "scope", "collection", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("<md>bucket.scope.collection._txn:atr-0-#14</md>", redacted.String())
	redacted = newLoggableATRKey("bucket", "", "collection", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("<md>bucket._default.collection._txn:atr-0-#14</md>", redacted.String())
	redacted = newLoggableATRKey("bucket", "scope", "", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("<md>bucket.scope._default._txn:atr-0-#14</md>", redacted.String())
	redacted = newLoggableATRKey("bucket", "", "", []byte("_txn:atr-0-#14"))
	suite.Assert().Equal("<md>bucket._default._default._txn:atr-0-#14</md>", redacted.String())
	SetLogRedactionLevel(RedactNone)
}

func loggerIDShort(txnID, attemptID string) string {
	return fmt.Sprintf("%s/%s", txnID[:5], attemptID[:5])
}
