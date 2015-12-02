package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.cassandra._
import scala.concurrent.{ExecutionContext, Future}

private[sdbc] trait ExecutableMethods extends base.ExecutableMethods[Session, Execute] {

  def executeAsync[Key](
    key: Key
  )(implicit ev: Executable[Key],
    connection: Session,
    ec: ExecutionContext
  ): Future[Unit] = {
    ev.execute(key).executeAsync()
  }

}
