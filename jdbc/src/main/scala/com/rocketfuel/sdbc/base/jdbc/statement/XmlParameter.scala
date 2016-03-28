package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.jdbc.DBMS
import java.sql.Types
import scala.xml.{Elem, NodeSeq}

trait XmlParameter {
  self: DBMS with StringParameter =>

  implicit val NodeSeqParameter: Parameter[NodeSeq] = {
    (nodes: NodeSeq) =>
      val asString = nodes.toString()
      (statement: PreparedStatement, columnIndex: Int) =>
        statement.setObject(columnIndex + 1, asString, Types.SQLXML)
        statement
  }

  implicit val ElemParameter: Parameter[Elem] = {
    (nodes: Elem) =>
      val asString = nodes.toString()
      (statement: PreparedStatement, columnIndex: Int) =>
        statement.setObject(columnIndex + 1, asString, Types.SQLXML)
        statement
  }

}
