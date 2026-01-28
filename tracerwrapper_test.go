package gocbcore

import "testing"

func (suite *UnitTestSuite) TestTracerWrapperSemanticConventions() {
	suite.T().Run("OnlyLegacyEmittedByDefault", func(t *testing.T) {
		tracer := newTestTracer()
		tw := newTracerWrapper(tracer, nil)
		suite.Require().True(tw.includeLegacyConventions)
		suite.Require().False(tw.includeStableConventions)

		span := tw.StartSpan(nil, "test-span")

		suite.Require().True(span.includeLegacyConventions)
		suite.Require().False(span.includeStableConventions)

		span.SetClusterName("test-cluster")
		span.End()

		s := tracer.Spans[nil][0]
		suite.Assert().Contains(s.Tags, "db.couchbase.cluster_name")
		suite.Assert().Equal("test-cluster", s.Tags["db.couchbase.cluster_name"])
		suite.Assert().NotContains(s.Tags, "couchbase.cluster.name")
	})

	suite.T().Run("DatabaseOptInEmitsStableOnly", func(t *testing.T) {
		tracer := newTestTracer()
		tw := newTracerWrapper(tracer, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
		})
		suite.Require().False(tw.includeLegacyConventions)
		suite.Require().True(tw.includeStableConventions)

		span := tw.StartSpan(nil, "test-span")

		suite.Require().False(span.includeLegacyConventions)
		suite.Require().True(span.includeStableConventions)

		span.SetClusterName("test-cluster")
		span.End()

		s := tracer.Spans[nil][0]
		suite.Assert().NotContains(s.Tags, "db.couchbase.cluster_name")
		suite.Assert().Contains(s.Tags, "couchbase.cluster.name")
		suite.Assert().Equal("test-cluster", s.Tags["couchbase.cluster.name"])
	})

	suite.T().Run("DatabaseDupOptInEmitsBoth", func(t *testing.T) {
		tracer := newTestTracer()
		tw := newTracerWrapper(tracer, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabaseDup,
		})
		suite.Require().True(tw.includeLegacyConventions)
		suite.Require().True(tw.includeStableConventions)

		span := tw.StartSpan(nil, "test-span")

		suite.Require().True(span.includeLegacyConventions)
		suite.Require().True(span.includeStableConventions)

		span.SetClusterName("test-cluster")
		span.End()

		s := tracer.Spans[nil][0]
		suite.Assert().Contains(s.Tags, "db.couchbase.cluster_name")
		suite.Assert().Equal("test-cluster", s.Tags["db.couchbase.cluster_name"])
		suite.Assert().Contains(s.Tags, "couchbase.cluster.name")
		suite.Assert().Equal("test-cluster", s.Tags["couchbase.cluster.name"])
	})

	suite.T().Run("DatabaseDupTakesPrecedence", func(t *testing.T) {
		tracer := newTestTracer()
		tw := newTracerWrapper(tracer, []ObservabilitySemanticConvention{
			ObservabilitySemanticConventionDatabase,
			ObservabilitySemanticConventionDatabaseDup,
		})
		suite.Require().True(tw.includeLegacyConventions)
		suite.Require().True(tw.includeStableConventions)

		span := tw.StartSpan(nil, "test-span")

		suite.Require().True(span.includeLegacyConventions)
		suite.Require().True(span.includeStableConventions)

		span.SetClusterName("test-cluster")
		span.End()

		s := tracer.Spans[nil][0]
		suite.Assert().Contains(s.Tags, "db.couchbase.cluster_name")
		suite.Assert().Equal("test-cluster", s.Tags["db.couchbase.cluster_name"])
		suite.Assert().Contains(s.Tags, "couchbase.cluster.name")
		suite.Assert().Equal("test-cluster", s.Tags["couchbase.cluster.name"])
	})
}
