package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.cassandra._
import scala.concurrent.{ExecutionContext, Future}

private[sdbc] trait Executable extends base.Executable {
  self: Cassandra =>

  def executeAsync[Key](
    key: Key
  )(implicit ev: Executable[Key],
    connection: Connection,
    ec: ExecutionContext
  ): Future[Unit] = {
    ev.execute(key).executeAsync()
  }

}
