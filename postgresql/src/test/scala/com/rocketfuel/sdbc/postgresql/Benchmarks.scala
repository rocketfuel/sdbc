package com.rocketfuel.sdbc.postgresql

import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID
import com.rocketfuel.sdbc.PostgreSql._
import org.apache.commons.lang3.time.StopWatch
import org.scalatest.BeforeAndAfterEach
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class Benchmarks
  extends PostgreSqlSuite
  with GeneratorDrivenPropertyChecks
  with BeforeAndAfterEach {

  val rowCount = 10000

  val repetitions =
    config.getInt("benchmark.repetitions")

  val warmUps =
    config.getInt("benchmark.warm_ups")

  var values: Vector[TestTable] = {

    val r = new util.Random()

    val randomClasses =
      for {
        i <- Range(0, rowCount)
      } yield {
        val str1Length = r.nextInt(20)
        val str1 = r.nextString(str1Length)
        val uuid = UUID.randomUUID()
        val str2Length = r.nextInt(20)
        val str2 = r.nextString(str2Length)
        TestTable(0, str1, uuid, str2)
      }

    randomClasses.toVector
  }

  def averageTime(repetitions: Int)(action: => Unit)(between: => Unit): Long = {

    //warmups
    for (repetition <- 0 until warmUps) {
      action
      between
    }

    val watch = new StopWatch()

    val times = Array.ofDim[Long](repetitions)

    for (repetition <- 0 until repetitions) {
      watch.start()
      action
      watch.stop()
      times(repetition) = watch.getTime
      watch.reset()
      between
    }

    times.sum[Long] / times.length
  }

  override protected def beforeEach(): Unit = {
    withPg {implicit connection =>
      TestTable.create.update()
      connection.commit()
    }
  }

  override protected def afterEach(): Unit = {
    withPg {implicit connection =>
      TestTable.drop.execute()
      connection.commit()
    }
  }

  ignore("test JDBC batch insert") {implicit connection =>
    val p = connection.prepareStatement(TestTable.insertJdbc)

    for (v <- values) v.addBatch(p)

    val insertedRowCount = p.executeBatch()

    connection.commit()

    assertResult(values.size)(insertedRowCount.sum[Int])
  }

  ignore("time JDBC batch insert") {implicit connection =>

    val insertDuration = averageTime(repetitions) {

      val p = connection.prepareStatement(TestTable.insertJdbc)

      for (v <- values) v.addBatch(p)

      p.executeBatch()

      connection.commit()

    }{
      TestTable.truncate.execute()
      connection.commit()
    }

    println(s"JDBC batch insert took $insertDuration ms.")
  }

  ignore("test JDBC select") {implicit connection =>
    val batch = values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}

    batch.iterator()

    val selectedRows = Array.ofDim[TestTable](rowCount)

    var selectedIndex: Int = 0

    val s = connection.prepareStatement(TestTable.select.queryText)

    val rs = s.executeQuery()

    while (rs.next()) {
      selectedRows(selectedIndex) = rs
      selectedIndex += 1
    }

    for ((TestTable(_, str1, uuid, str2), TestTable(_, str1_, uuid_, str2_)) <- values.zip(selectedRows)) {
      assert(str1 == str1_)
      assert(uuid == uuid_)
      assert(str2_ == str2_)
    }

  }

  ignore("time JDBC select") {implicit connection =>
    values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}.iterator()

    connection.commit()

    val selectDuration = averageTime(repetitions) {

      val selectedRows = collection.mutable.Buffer.empty[TestTable]

      val s = connection.prepareStatement(TestTable.select.queryText)

      val rs = s.executeQuery()

      while (rs.next()) {
        selectedRows.append(rs)
      }

    }(() => ())

    println(s"JDBC select took $selectDuration ms.")

  }

  ignore("time com.rocketfuel.sql batch insert") {implicit connection =>

    val insertDuration = averageTime(repetitions) {
      values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}.iterator()
      connection.commit()
    }{
      TestTable.truncate.execute()
      connection.commit()
    }

    println(s"com.rocketfuel.sql batch insert took $insertDuration ms.")

  }

  ignore("test com.rocketfuel.batch insert") {implicit connection =>

    val batch = values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}

    val insertedRows = batch.iterator()

    connection.commit()

    assert(insertedRows.sum == values.size.toLong)

  }

  ignore("time com.rocketfuel.sql select") {implicit connection =>

    values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}.iterator()

    connection.commit()

    val selectDuration = averageTime(repetitions) {
      TestTable.select.iterator()
    }(() => ())

    println(s"com.rocketfuel.sql select took $selectDuration ms.")

  }

  ignore("test com.rocketfuel.sql select") {implicit connection =>

    values.foldLeft(TestTable.batchInsert){case (b, v) => v.addBatch(b)}.iterator()

    connection.commit()

    val selectedRows = TestTable.select.iterator().toSeq

    for ((TestTable(_, str1, uuid, str2), TestTable(_, str1_, uuid_, str2_)) <- values.zip(selectedRows)) {
      assert(str1 == str1_)
      assert(uuid == uuid_)
      assert(str2_ == str2_)
    }

  }

  case class TestTable(
    id: Long,
    str1: String,
    uuid: UUID,
    str2: String
  ) {
    def addBatch(b: Batch): Batch = {
      b.addBatch(
        "str1" -> str1,
        "uuid" -> uuid,
        "str2" -> str2
      )
    }

    def addBatch(p: PreparedStatement): Unit = {
      p.setString(1, str1)
      p.setObject(2, uuid)
      p.setString(3, str2)
      p.addBatch()
    }
  }

  object TestTable {

    implicit def apply(row: Row): TestTable = {
      val id = row[Long]("id")
      val str1 = row[String]("str1")
      val uuid = row[UUID]("uuid")
      val str2 = row[String]("str2")

      TestTable(id, str1, uuid, str2)
    }

    implicit def apply(row: ResultSet): TestTable = {
      val id = row.getLong("id")
      val str1 = row.getString("str1")
      val uuid = row.getObject("uuid").asInstanceOf[UUID]
      val str2 = row.getString("str2")

      TestTable(id, str1, uuid, str2)
    }

    val create = {
      val queryText =
        s"""CREATE TABLE test (
         |  id bigserial PRIMARY KEY,
         |  str1 text,
         |  uuid uuid,
         |  str2 text
         |);
       """.stripMargin

      Update(queryText, hasParameters = false)
    }

    val insert = {
      val queryText =
        """INSERT INTO TEST
          |(str1, uuid, str2)
          |VALUES
          |($str1, $uuid, $str2)
        """.stripMargin
      Update(queryText)
    }

    val batchInsert = {
      val queryText =
        """INSERT INTO TEST
          |(str1, uuid, str2)
          |VALUES
          |(@str1, @uuid, @str2)
        """.stripMargin
      Batch(queryText)
    }

    val insertJdbc =
      """INSERT INTO TEST
        |(str1, uuid, str2)
        |VALUES
        |(?, ?, ?)
      """.stripMargin

    val select = Select[TestTable]("SELECT * FROM test ORDER BY id;", hasParameters = false)

    val drop =
      Execute("DROP TABLE test;", hasParameters = false)

    val truncate =
      Execute("TRUNCATE TABLE test RESTART IDENTITY;")

  }

}
