# Release Notes

## Version 10.0.9 (15 March 2022)

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1248](GOCBC-1248):
  Fixed issue where a hard close of a memdclient during a graceful close could trigger a panic.
* [https://issues.couchbase.com/browse/GOCBC-1258](GOCBC-1258):
  Fixed issue where log redaction tags were not closed correctly.

## Version 10.0.8 (15 February 2022)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1238](GOCBC-1238):
  Add config option for setting connection reader buffer size.
* [https://issues.couchbase.com/browse/GOCBC-1242](GOCBC-1242):
  Update to flush the DCP buffer on EOF originating from remote, rather than failing the DCP requests with EOF straight away.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1232](GOCBC-1232):
  Fixed issue where DCP stream End could race with request cancellation (due to rebalance, etc...).
* [https://issues.couchbase.com/browse/GOCBC-1233](GOCBC-1233):
  Fixed issue where Agent close could hang if called whilst auth request in flight.

## Version 10.0.7 (24 January 2022)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1216](GOCBC-1216):
  Add support for missing memcached status code 0x8d
* [https://issues.couchbase.com/browse/GOCBC-1222](GOCBC-1222):
  Updated memcached connections to use a `sync.Pool` for buffers for readers, to help reduce memory footprint.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1214](GOCBC-1214):
  Fixed issue where nodes "actual" IP could be used for internal config instead of seed address when `NoTLSSeedNode` in use.

## Version 10.0.6 (14 December 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1190](GOCBC-1190):
  Added internal stability support for sending queries to specific nodes.
* [https://issues.couchbase.com/browse/GOCBC-1196](GOCBC-1196):
* Added error body and status code to analytics, query, search, view errors.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1205](GOCBC-1205):
  Fixed issue where tracer spans were not always being finished.
* [https://issues.couchbase.com/browse/GOCBC-1206](GOCBC-1206):
  Fixed issue where metrics were always incorrectly reporting very short durations for operations.
* [https://issues.couchbase.com/browse/GOCBC-1208](GOCBC-1208):
  Fixed issue where cluster config polling would fallback to HTTP polling even when there was no bucket.
* [https://issues.couchbase.com/browse/GOCBC-1209](GOCBC-1209):
  Fixed issue where the ns server connection string scheme wouldn't work for DCP.


## Version 10.0.5 (16 November 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1179](GOCBC-1179):
  Gracefully close memdclients on pipeline shutdown/reconnect.
* [https://issues.couchbase.com/browse/GOCBC-1180](GOCBC-1180):
  Added support for the ns_server connection string scheme and seed (i.e. localhost) poller.
* [https://issues.couchbase.com/browse/GOCBC-1181](GOCBC-1181):
  Added support for `ReconfigureSecurity` function.
* [https://issues.couchbase.com/browse/GOCBC-1182](GOCBC-1182):
  Request error map v2 from the server.
* [https://issues.couchbase.com/browse/GOCBC-1193](GOCBC-1193):
  Added the response body to query errors.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1194](GOCBC-1194):
  Fixed issue where we wouldn't try to build a route config with all seed nodes for default network type before trying external network type.

## Version 10.0.4 (19 October 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1178](GOCBC-1178):
  Don't remove poller controller watcher from cluster config updates.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1177](GOCBC-1177):
  Fixed issue where a connection being closed by the server during bootstrap could cause the SDK to loop reconnect without backoff.


## Version 10.0.3 (21 September 2021)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1162](GOCBC-1162):
  Added support for initially bootstrapping the SDK over nonTLS when TLS is in use.
* [https://issues.couchbase.com/browse/GOCBC-1169](GOCBC-1169):
  Updated query streamer so that additional calls to `NextRow` return nil rather than panic.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1160](GOCBC-1160):
  Fixed issue where HTTP header used for user impersonation was incorrect.
* [https://issues.couchbase.com/browse/GOCBC-1163](GOCBC-1163):
  Fixed issue where cluster config parsing would check existence of wrong ports for TLS (although then assign correct ports).

## Version 10.0.2 (17 August 2021)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1146](GOCBC-1146):
  Added support for user impersonation to non-KV services.
* [https://issues.couchbase.com/browse/GOCBC-1148](GOCBC-1148):
  Added support for forcibly reconnecting all connections.
* [https://issues.couchbase.com/browse/GOCBC-1150](GOCBC-1150):
  Update user impersonation options for KV to use a string rather than []byte.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1139](GOCBC-1139):
  Fixed issue where DCP agent would try to use SCRAM auth with TLS enabled, causing LDAP usage to always fail bootstrap.
* [https://issues.couchbase.com/browse/GOCBC-1147](GOCBC-1147):
  Fixed issue where failing to fetch the error map during bootstrap would lead to bootstrap hanging.

## Version 10.0.1 (15 July 2021)

### Fixed Issues

* Fixed issue where modules file contained incorrect gocbcore version.

## Version 10.0.0 (15 July 2021) (Do not use, see v10.0.1)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-901](GOCBC-901):
  Broke the `AgentConfig` up into grouped components.
* [https://issues.couchbase.com/browse/GOCBC-1008](GOCBC-1008):
  Updated mutate in to return cas mismatch error rather than document exists when doing a replace.
* [https://issues.couchbase.com/browse/GOCBC-1062](GOCBC-1062):
  Added support for DCP snapshot marker v2 and v2.1.
* [https://issues.couchbase.com/browse/GOCBC-1081](GOCBC-1081):
  During CCCP polling don't retry request if the error is request cancelled.
* [https://issues.couchbase.com/browse/GOCBC-1130](GOCBC-1130):
  Updated Query error handling to return an authentication error on error code 13104.
* [https://issues.couchbase.com/browse/GOCBC-1087](GOCBC-1087):
  Added support for communicating with Eventing and Backup services.
* [https://issues.couchbase.com/browse/GOCBC-1093](GOCBC-1093):
  Added support for `RevEpoch` in bucket configs.
* [https://issues.couchbase.com/browse/GOCBC-1044](GOCBC-1044):
* [https://issues.couchbase.com/browse/GOCBC-1128](GOCBC-1128):
  Added `Meter` interface and operation level response latency metric.
* [https://issues.couchbase.com/browse/GOCBC-1133](GOCBC-1133):
  Remove `ViewQuery` from `AgentGroup`.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1135](GOCBC-1135):
  Fixed issue where cmd traces could be ended twice in some scenarios when operation was cancelled.

## Version 9.1.5 (15 June 2021)

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1095](GOCBC-1095):
  Fixed issue where SDK was parsing view error contents incorrectly.
* [https://issues.couchbase.com/browse/GOCBC-1102](GOCBC-1102):
  Fixed issue where `WaitUntilReady` wouldn't recover if one of the HTTP based services returned an error.
* [https://issues.couchbase.com/browse/GOCBC-1106](GOCBC-1106):
* [https://issues.couchbase.com/browse/GOCBC-1112](GOCBC-1112):
  Fixed issues where fts responses were being parsed incorrectly.
* [https://issues.couchbase.com/browse/GOCBC-1127](GOCBC-1127):
  Fixed issue where query errors could be parsed incorrectly.

## Version 9.1.4 (20 April 2021)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1071](GOCBC-1071):
  Updated SDK to use new protocol level changes for get collection id.
* [https://issues.couchbase.com/browse/GOCBC-1068](GOCBC-1068):
  Dropped log level to warn for when applying a cluster config object is preempted.
* [https://issues.couchbase.com/browse/GOCBC-1079](GOCBC-1079):
  During bootstrap don't retry authentication if the error is request cancelled.
* [https://issues.couchbase.com/browse/GOCBC-1081](GOCBC-1081):
  During CCCP polling don't retry request if the error is request cancelled.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1080](GOCBC-1080):
  Fixed issue where SDK would always rebuild connections on first cluster config fetched against server 7.0.
* [https://issues.couchbase.com/browse/GOCBC-1082](GOCBC-1082):
  Fixed issue where bootstrapping a node during an SDK wide reconnect would cause a delay in connecting to that node.
* [https://issues.couchbase.com/browse/GOCBC-1088](GOCBC-1088):
  Fixed issue where the poller controller could deadlock if a node reported a bucket not found at the same time as CCCP successfully fetched a cluster config for the first time.
  
## Version 9.1.3 (16 March 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1056](GOCBC-1056):
  Various performance improvements to reduce CPU level.
* [https://issues.couchbase.com/browse/GOCBC-1068](GOCBC-1068):
  Dropped the log level for preempted config updates.
* [https://issues.couchbase.com/browse/GOCBC-940](GOCBC-940):
  Updated the tracing interfaces and orphaned response logging output.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1066](GOCBC-1066):
  Fixed issue which could cause the config pollers to panic.

## Version 9.1.2 (16 February 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1041](GOCBC-1041):
  Dropped the log level for memdclient read failures to warn, from error.
* [https://issues.couchbase.com/browse/GOCBC-1046](GOCBC-1046):
  Added `MaxTTl` to `ManifestCollection`.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1042](GOCBC-1042):
  Fixed issue where bucket names were not being correctly escaped.
* [https://issues.couchbase.com/browse/GOCBC-1050](GOCBC-1050):
  Fixed issue where the diagnostics component could panic if an operation was cancelled by the user after it had already been internally cancelled.

## Version 9.1.1 (19 January 2021)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1032](GOCBC-1032):
  Added support for bucket capability support verification to agent, at API stability internal.
* [https://issues.couchbase.com/browse/GOCBC-1030](GOCBC-1030):
  Added support for internal cancellation of bootstrap before completion, allowing pipeline clients to shutdown without waiting for bootstrap to complete (such as on connection takeover).

  Added support to fallback to http config fetching if select bucket fails with a valid fallback error, allowing for faster config fetching against non-kv nodes.

## Version 9.1.0 (15 December 2020)

### New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-854](GOCBC-854):
Added support for user impersonation.
* [https://issues.couchbase.com/browse/GOCBC-1013](GOCBC-1013):
Added support for `StatsKeys` and `StatsChunks` to `SingleServerStats` to support responses for stats keys such as `connections` which contain complex objects per packet.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1016](GOCBC-1016):
Fixed issue where creating an agent with no bucket and a non-default port HTTP address could lead to a panic in `WaitForReady`.
(Note: `WaitForReady` will *never* return success in this scenario)
* [https://issues.couchbase.com/browse/GOCBC-1028](GOCBC-1028):
Fixed issue where bootstrapping against a non-kv node could never successfully fully connect.
