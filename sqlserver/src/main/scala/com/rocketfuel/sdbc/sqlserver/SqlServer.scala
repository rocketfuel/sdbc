package com.rocketfuel.sdbc.sqlserver

import com.rocketfuel.sdbc.base.jdbc._

/*
Note that in a result set, sql server (or jtds) doesn't do a good job of reporting the types
of values being delivered.

nvarchar could be:
string, date, time, datetime2, datetimeoffset

ntext could be:
string, xml

varbinary could be:
varbinary, hierarchyid
 */
trait SqlServer
  extends DBMS
  with Getters
  with Updaters
  with Setters
  with MultiQuery
  with JdbcConnection {

  override val connectionTestQuery: Option[String] = Some("SELECT 1")

  type HierarchyId = com.rocketfuel.sdbc.sqlserver.HierarchyId

  val HierarchyId = com.rocketfuel.sdbc.sqlserver.HierarchyId

  type HierarchyNode = com.rocketfuel.sdbc.sqlserver.HierarchyNode

  val HierarchyNode = com.rocketfuel.sdbc.sqlserver.HierarchyNode

  trait syntax
    extends super.syntax
      with MultiQueryable.syntax

  override val syntax = new syntax {}

}
