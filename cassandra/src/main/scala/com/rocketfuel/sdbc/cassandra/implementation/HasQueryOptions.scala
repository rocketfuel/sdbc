package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.cassandra.QueryOptions

private[sdbc] trait HasQueryOptions {
  def queryOptions: QueryOptions
}
