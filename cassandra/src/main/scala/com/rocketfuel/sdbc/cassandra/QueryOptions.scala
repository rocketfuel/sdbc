package com.rocketfuel.sdbc.cassandra

import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.protocol.internal.request.query
import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import java.time.Instant

case class QueryOptions(
  consistencyLevel: ConsistencyLevel = DefaultConsistencyLevel.fromCode(query.QueryOptions.DEFAULT.consistency),
  serialConsistencyLevel: ConsistencyLevel = DefaultConsistencyLevel.fromCode(query.QueryOptions.DEFAULT.serialConsistency),
  defaultTimestamp: Option[Instant] = None,
  pageSize: Int = query.QueryOptions.DEFAULT.pageSize,
  idempotent: Option[Boolean] = None,
  tracing: Boolean = false
) {

  def set(statement: BoundStatement): Unit = {
    statement.setConsistencyLevel(consistencyLevel)
    statement.setSerialConsistencyLevel(serialConsistencyLevel)
    for (i <- defaultTimestamp) {
      statement.setQueryTimestamp(i.toEpochMilli * 1000L)
    }
    statement.setPageSize(pageSize)
    idempotent.foreach(statement.setIdempotent(_))
    statement.setTracing(tracing)
  }

}

object QueryOptions {
  val default = QueryOptions()
}
