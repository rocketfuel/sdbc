package com.rocketfuel.sdbc.h2.benchmarks

import com.rocketfuel.sdbc.H2._
import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID

case class TestTable(
  id: Long,
  str1: String,
  uuid: UUID,
  str2: String
) {
  def addBatch(p: PreparedStatement): Unit = {
    p.setString(1, str1)
    p.setObject(2, uuid)
    p.setString(3, str2)
    p.addBatch()
  }
}

object TestTable {

  def apply(row: ResultSet): TestTable = {
    val id = row.getLong("id")
    val str1 = row.getString("str1")
    val uuid = row.getObject("uuid").asInstanceOf[UUID]
    val str2 = row.getString("str2")

    TestTable(id, str1, uuid, str2)
  }

  val create = {
    val queryText =
      s"""CREATE TABLE IF NOT EXISTS test (
          |  id bigserial PRIMARY KEY,
          |  str1 text,
          |  uuid uuid,
          |  str2 text
          |);
       """.stripMargin

    Ignore(queryText)
  }

  val insert = {
    val queryText =
      """INSERT INTO TEST
        |(str1, uuid, str2)
        |VALUES
        |(@str1, @uuid, @str2)
      """.stripMargin
    Insert(queryText)
  }

  val insertJdbc =
    insert.queryText

  val select =
    Select[TestTable]("SELECT * FROM test;")

  val drop =
    Ignore("DROP TABLE IF EXISTS test;")

  val truncate =
    Ignore("TRUNCATE TABLE test;")

  object doobieMethods {
    import doobie.enum.jdbctype
    import doobie.free.{preparedstatement => PS, resultset => RS}
    import doobie.imports._

    implicit val uuidMeta: Meta[UUID] =
      Meta.basic1[UUID](
        jdbcType = jdbctype.JavaObject,
        jdbcSourceSecondary0 = List(),
        get0 = (rs, ix) => rs.getObject(ix).asInstanceOf[UUID],
        set0 = PS.setObject,
        update0 = RS.updateObject
      )

    val select: Query0[TestTable] =
      sql"SELECT * FROM test".query[TestTable]

    val insert =
      Update[(String, UUID, String)](TestTable.insertJdbc)
  }

}
