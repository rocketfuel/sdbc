package com.rocketfuel.sdbc.cassandra

import com.datastax.driver.core.policies.{DefaultRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{ConsistencyLevel, QueryOptions => CQueryOptions}

case class QueryOptions(
  consistencyLevel: ConsistencyLevel = CQueryOptions.DEFAULT_CONSISTENCY_LEVEL,
  serialConsistencyLevel: ConsistencyLevel = CQueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL,
  defaultTimestamp: Option[Long] = None,
  fetchSize: Int = CQueryOptions.DEFAULT_FETCH_SIZE,
  idempotent: Boolean = CQueryOptions.DEFAULT_IDEMPOTENCE,
  retryPolicy: RetryPolicy = DefaultRetryPolicy.INSTANCE,
  tracing: Boolean = false
)

object QueryOptions {
  val default = QueryOptions()
}
