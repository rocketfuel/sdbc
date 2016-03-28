package com.rocketfuel.sdbc.base.jdbc

trait ConnectionMethods {
  self: DBMS =>

  implicit class ConnectionMethods(c: Connection) {

    private implicit val connection = c

//    def query[A](
//      queryText: String
//    )(implicit statementConverter: StatementConverter.OuterAux[A]): statementConverter.InnerResult = {
//      val QueryResult(close, results) = Query[A](queryText)
//      close.close()
//      results
//    }

  }

}
