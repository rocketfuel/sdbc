package com.rocketfuel.sdbc.sqlserver.jdbc.implementation

import java.io.{InputStream, Reader}
import java.sql.PreparedStatement
import java.time.{LocalTime, OffsetDateTime}
import java.util.UUID
import com.rocketfuel.sdbc.base.CISet
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.jdbc.{HierarchyNodeImplicits, HierarchyId}
import scodec.bits.ByteVector

import scala.xml.Node

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
private[sdbc] abstract class SqlServer
  extends DBMS
  with Setters
  with Getters
  with Updaters
  with HierarchyNodeImplicits {

  override def driverClassName = "net.sourceforge.jtds.jdbc.Driver"
  override def dataSourceClassName ="net.sourceforge.jtds.jdbcx.JtdsDataSource"
  override def jdbcSchemes = CISet("jtds:sqlserver")
  override def productName: String = "Microsoft SQL Server"
  override val supportsIsValid = false


  override implicit val ParameterSetter: ParameterSetter = new ParameterSetter {
    /**
     * Pattern match to get the IsParameter instance for
     * a value, and then call setParameter.
     *
     * This method is to be implemented on a per-DBMS basis.
     * @param preparedStatement
     * @param parameterIndex
     * @param parameter
     */
    override def setAny(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Any): Unit = {
      parameter match {
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
        case b: Byte =>
          setParameter[Byte](preparedStatement, parameterIndex, b)
        case b: String =>
          setParameter[String](preparedStatement, parameterIndex, b)
        case b: java.sql.Time =>
          setParameter[java.sql.Time](preparedStatement, parameterIndex, b)
        case b: java.sql.Timestamp =>
          setParameter[java.sql.Timestamp](preparedStatement, parameterIndex, b)
        case b: OffsetDateTime =>
          setParameter[OffsetDateTime](preparedStatement, parameterIndex, b)
        case b: java.time.LocalTime =>
          setParameter[LocalTime](preparedStatement, parameterIndex, b)
        case b: Reader =>
          setParameter[Reader](preparedStatement, parameterIndex, b)
        case b: InputStream =>
          setParameter[InputStream](preparedStatement, parameterIndex, b)
        case b: UUID =>
          setParameter[UUID](preparedStatement, parameterIndex, b)
        case b: HierarchyId =>
          setParameter[HierarchyId](preparedStatement, parameterIndex, b)
        case b: Node =>
          setParameter[Node](preparedStatement, parameterIndex, b)
      }
    }
  }

  override implicit val ParameterGetter: Getter[ParameterValue] = {
    case (row: Row, columnIndex: Index) =>
      val ix = columnIndex(row)

      val columnType = row.columnTypes(columnIndex(row))

      columnType match {
        case "int" =>
          IntGetter(row, ix).map(ParameterValue)
        case "bit" =>
          BooleanGetter(row, ix)
        case "tinyint" =>
          ByteGetter(row, ix)
        case "smallint" =>
          ShortGetter(row, ix)
        case "bigint" =>
          LongGetter(row, ix)
        case "decimal" =>
          JavaBigDecimalGetter(row, ix)
        case "real" =>
          FloatGetter(row, ix)
        case "float" =>
          DoubleGetter(row, ix)
        case "datetime" | "smalldatetime" =>
          TimestampGetter(row, ix)
        case "binary" | "varbinary" | "image" =>
          ArrayByteGetter(row, ix)
        case "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" =>
          StringGetter(row, ix)
        case "uniqueidentifier" =>
          UUIDGetter(row, ix)
      }
  }

  override def toParameter(a: Any): Option[Any] = {
    a match {
      case null | None =>
        None
      case Some(a) =>
        Some(toParameter(a)).flatten
      case _ =>
        Some(toSqlServerParameter(a))
    }
  }

}
