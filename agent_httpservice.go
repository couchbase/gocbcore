package gocbcore

// N1QLQuery executes a N1QL query
func (agent *Agent) N1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	return agent.n1qlCmpt.N1QLQuery(opts)
}

// AnalyticsQuery executes an analytics query
func (agent *Agent) AnalyticsQuery(opts AnalyticsQueryOptions) (*AnalyticsRowReader, error) {
	return agent.analyticsCmpt.AnalyticsQuery(opts)
}

// SearchQuery executes a Search query
func (agent *Agent) SearchQuery(opts SearchQueryOptions) (*SearchRowReader, error) {
	return agent.searchCmpt.SearchQuery(opts)
}
