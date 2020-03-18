package gocbcore

// N1QLQuery executes a N1QL query
func (agent *Agent) N1QLQuery(opts N1QLQueryOptions) (*N1QLRowReader, error) {
	return agent.n1qlCmpt.N1QLQuery(opts)
}
