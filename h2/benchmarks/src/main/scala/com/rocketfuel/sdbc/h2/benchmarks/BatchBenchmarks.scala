package com.rocketfuel.sdbc.h2.benchmarks

import com.rocketfuel.sdbc.H2._
import java.util.UUID
import org.openjdk.jmh.annotations.{BenchmarkMode, OutputTimeUnit, Setup, _}
import java.util.concurrent.TimeUnit
import scalaz.effect.IO
import scalaz.std.vector._
import shapeless.syntax.std.tuple._

@State(Scope.Thread)
class BatchBenchmarks {

  implicit val connection =
    Connection.get("jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=0")

  @Param(Array("0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512"))
  var valueCount: Int = _

  var values: Vector[TestTable] = _

  var valuesDoobie: Vector[(String, UUID, String)] = _

  var batch: Batch = _

  def createValues(): Vector[TestTable] = {
    val r = new util.Random()

    val randomClasses =
      for {
        i <- 0 until valueCount
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

  @Setup(Level.Iteration)
  def setup(): Unit = {
    values = createValues()
    valuesDoobie = values.map(_.drop(1))
    batch = BatchBenchmarks.createBatch(values)
    TestTable.create.execute()
  }

  @TearDown(Level.Iteration)
  def teardown(): Unit = {
    TestTable.drop.execute()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def jdbc(): Unit = {
    val p = connection.prepareStatement(TestTable.insertJdbc)

    for (v <- values) v.addBatch(p)

    p.executeBatch()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sdbc(): Unit = {
    batch.batch()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def doobie(): Unit = {
    TestTable.doobieMethods.insert.
      updateMany(valuesDoobie).
      transK[IO].run(connection).unsafePerformIO()
  }

}

object BatchBenchmarks {
  def createBatch(values: Seq[TestTable]): Batch = {
    values.foldLeft(TestTable.batchInsert){case (b, v) => b.addProduct(v)}
  }
}
