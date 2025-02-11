package gocbcore

type columnarClientMux struct {
	epList []routeEndpoint

	uuid  string
	revID int64

	srcConfig routeConfig

	tlsConfig *dynTLSConfig
	auth      AuthProvider
}

func newColumnarClientMux(cfg *routeConfig, endpoints []routeEndpoint, tlsConfig *dynTLSConfig, auth AuthProvider) *columnarClientMux {
	return &columnarClientMux{
		epList: endpoints,

		uuid:  cfg.uuid,
		revID: cfg.revID,

		srcConfig: *cfg,

		tlsConfig: tlsConfig,
		auth:      auth,
	}
}
