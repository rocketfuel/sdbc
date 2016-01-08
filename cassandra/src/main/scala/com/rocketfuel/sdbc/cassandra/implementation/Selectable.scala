package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base
import scala.concurrent.{ExecutionContext, Future}

private[sdbc] trait Selectable extends base.Selectable {
  self: Cassandra =>

  def iteratorAsync[Key, Value](
    key: Key
  )(implicit ev: Selectable[Key, Value],
    connection: Connection,
    ec: ExecutionContext
  ): Future[Iterator[Value]] = {
    ev.select(key).iteratorAsync()
  }

}
