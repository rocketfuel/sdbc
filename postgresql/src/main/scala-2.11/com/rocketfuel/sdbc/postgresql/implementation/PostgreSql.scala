package com.rocketfuel.sdbc.postgresql.implementation

import java.io.{InputStream, Reader}
import java.sql.PreparedStatement
import java.util.UUID

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.postgresql.implementation
import org.json4s._
import org.postgresql.util.PGobject
import scodec.bits.ByteVector

import scala.xml.Node

private[sdbc] abstract class PostgreSql
  extends PostgreSqlCommon
  with SeqParameter
  with SeqGetter {

  type QSeq[T] = com.rocketfuel.sdbc.postgresql.implementation.QSeq[T]
  val QSeq = implementation.QSeq

  override implicit val ParameterGetter: Getter[ParameterValue] = {
    (row: Row, columnIndex: Index) =>
      val ix = columnIndex(row)

      val columnType = row.columnTypes(columnIndex(row))

      columnType match {
        case "int4" | "serial" =>
          IntGetter(row, ix).map(ParameterValue)
        case "bool" =>
          BooleanGetter(row, ix)
        case "int2" =>
          ShortGetter(row, ix)
        case "int8" | "bigserial" =>
          LongGetter(row, ix)
        case "numeric" =>
          JavaBigDecimalGetter(row, ix)
        case "float4" =>
          FloatGetter(row, ix)
        case "float8" =>
          DoubleGetter(row, ix)
        case "time" =>
          LocalTimeGetter(row, ix)
        case "timetz" =>
          OffsetTimeGetter(row, ix)
        case "date" =>
          DateGetter(row, ix)
        case "timestamp" =>
          TimestampGetter(row, ix)
        case "timestamptz" =>
          OffsetDateTimeGetter(row, ix)
        case "bytea" =>
          ArrayByteGetter(row, ix)
        case "varchar" | "bpchar" | "text" =>
          StringGetter(row, ix)
        case "uuid" =>
          UUIDGetter(row, ix)
        case "xml" =>
          XMLGetter(row, ix)
        case "json" | "jsonb" =>
          JValueGetter(row, ix)
        case "interval" =>
          PGIntervalGetter(row, ix)
        case "inet" =>
          InetAddressGetter(row, ix)
        case "hstore" =>
          MapGetter(row, ix)
        case "ltree" =>
          LTreeGetter(row, ix)
        case array if array.startsWith("_") =>
          for {
            jdbcArray <- Option(row.getArray(Index(ix)(row)))
          } yield {
            val innerRows = jdbcArray.getResultSet().iterator()
            val innerRowsAsParameters = innerRows.map(r => ParameterGetter(r, 1)).toSeq

            ParameterValue(QSeq(innerRowsAsParameters, array.tail))
          }
      }
  }

  override implicit val ParameterSetter: ParameterSetter = new ParameterSetter {
    /**
     * Pattern match on parameters to get the IsParameter instance for
     * each value, and then call setParameter.
     * @param preparedStatement
     * @param parameterIndex
     * @param parameter
     */
    override def setAny(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Any): Unit = {
      parameter match {
        case b: ParameterValue =>
          setAny(preparedStatement, parameterIndex, b.value)
        case b: Boolean =>
          setParameter[Boolean](preparedStatement, parameterIndex, b)
        case b: ByteVector =>
          setParameter[ByteVector](preparedStatement, parameterIndex, b)
        case b: java.sql.Date =>
          setParameter[java.sql.Date](preparedStatement, parameterIndex, b)
        case b: java.math.BigDecimal =>
          setParameter[java.math.BigDecimal](preparedStatement, parameterIndex, b)
        case b: Double =>
          setParameter[Double](preparedStatement, parameterIndex, b)
        case b: Float =>
          setParameter[Float](preparedStatement, parameterIndex, b)
        case b: Int =>
          setParameter[Int](preparedStatement, parameterIndex, b)
        case b: Long =>
          setParameter[Long](preparedStatement, parameterIndex, b)
        case b: Short =>
          setParameter[Short](preparedStatement, parameterIndex, b)
        case b: String =>
          setParameter[String](preparedStatement, parameterIndex, b)
        case b: java.sql.Time =>
          setParameter[java.sql.Time](preparedStatement, parameterIndex, b)
        case b: java.sql.Timestamp =>
          setParameter[java.sql.Timestamp](preparedStatement, parameterIndex, b)
        case b: Reader =>
          setParameter[Reader](preparedStatement, parameterIndex, b)
        case b: InputStream =>
          setParameter[InputStream](preparedStatement, parameterIndex, b)
        case b: UUID =>
          setParameter[UUID](preparedStatement, parameterIndex, b)
        case b: PGobject =>
          setParameter[PGobject](preparedStatement, parameterIndex, b)
        case b: java.util.Map[_, _] =>
          setParameter[java.util.Map[String, String]](preparedStatement, parameterIndex, b.asInstanceOf[java.util.Map[String, String]])
        case b: Node =>
          setParameter[Node](preparedStatement, parameterIndex, b)
        case b: JValue =>
          setParameter[PGobject](preparedStatement, parameterIndex, b)
        case q: QSeq[_] =>
          setParameter[QSeq[_]](preparedStatement, parameterIndex, q)
      }
    }
  }

}
