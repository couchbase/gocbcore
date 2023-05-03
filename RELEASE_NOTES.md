# Release Notes

## Version 9.1.10 (3 May 2023)

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1409](GOCBC-1409):
  Updated DCP OSO to use improved SeqNoAdvance.

## Version 9.1.9 (20 April 2023)

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1401](GOCBC-1401):
  Exposed SeqNo on DCP rollback error.

## Version 9.1.8 (18 January 2022)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1216](GOCBC-1216):
  Added support for memcached status code 0x8d.

## Version 9.1.7 (21 September 2021)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1162](GOCBC-1162):
  Added support for initially bootstrapping the SDK over nonTLS when TLS is in use.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1163](GOCBC-1163):
  Fixed issue where cluster config parsing would check existence of wrong ports for TLS (although then assign correct ports).

## Version 9.1.6 (17 August 2021)

###New Features and Behavioral Changes

* [https://issues.couchbase.com/browse/GOCBC-1148](GOCBC-1148):
  Added support for forcibly reconnecting all connections.

### Fixed Issues

* [https://issues.couchbase.com/browse/GOCBC-1139](GOCBC-1139):
  Fixed issue where DCP agent would try to use SCRAM auth with TLS enabled, causing LDAP usage to always fail bootstrap.
* [https://issues.couchbase.com/browse/GOCBC-1147](GOCBC-1147):
  Fixed issue where failing to fetch the error map during bootstrap would lead to bootstrap hanging.

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
