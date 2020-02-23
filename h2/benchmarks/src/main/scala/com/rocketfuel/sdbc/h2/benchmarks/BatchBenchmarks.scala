package com.rocketfuel.sdbc.h2.benchmarks

import cats.effect.{Blocker, IO}
import cats.instances.vector._
import com.rocketfuel.sdbc.H2._
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import shapeless.syntax.std.tuple._
import scala.util.Random

@State(Scope.Thread)
@Fork(value = 1)
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 5, time = 1)
class BatchBenchmarks {

  implicit val connection =
    Connection.get("jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=0")

  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)

  val xa = Blocker[IO].map { b =>
    Transactor.fromConnection[IO](connection, b)
  }

  @Param(Array("0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512"))
  var valueCount: Int = _

  var values: Vector[TestTable] = _

  var sdbcBatch: Batch = _

  var doobieBatch: ConnectionIO[Int] = _

  def createValues(): Vector[TestTable] = {
    val r = new Random()

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
    doobieBatch = BatchBenchmarks.createDoobieBatch(values.map(_.drop(1)))
    sdbcBatch = BatchBenchmarks.createSdbcBatch(values)
  }

  @Setup(Level.Invocation)
  def setupTable(): Unit = {
    TestTable.create.ignore()
    TestTable.truncate.ignore()
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
    sdbcBatch.batch()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def doobie(): Unit = {
    xa.use(doobieBatch.transact[IO]).unsafeRunSync()
  }

}

object BatchBenchmarks {
  def createSdbcBatch(values: Seq[TestTable]): Batch = {
    Batch(values.map(TestTable.insert.onProduct(_)): _*)
  }

  def createDoobieBatch(values: Vector[(String, UUID, String)]): ConnectionIO[Int] = {
    TestTable.doobieMethods.insert.updateMany(values)
  }
}
