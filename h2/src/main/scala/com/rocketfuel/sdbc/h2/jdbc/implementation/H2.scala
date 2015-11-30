package com.rocketfuel.sdbc.h2.jdbc.implementation

import java.io.{InputStream, Reader}
import java.nio.file.Path
import java.sql.{PreparedStatement, DriverManager}
import java.util.UUID
import com.rocketfuel.sdbc.base.{CompositeSetter$, CISet}
import com.rocketfuel.sdbc.base.jdbc.{ParameterSetter, DBMS}
import com.rocketfuel.sdbc.h2.jdbc.Serialized
import scodec.bits.ByteVector

private[sdbc] abstract class H2
  extends DBMS
  with Getters
  with Setters
  with Updaters
  with SerializedParameter
  with SeqParameter {

  override implicit val ParameterGetter: Getter[ParameterValue] = {
    (row: Row, ix: Index) =>
      val columnType = row.getMetaData.getColumnTypeName(ix(row) + 1)

      columnType match {
        case "INTEGER" =>
          IntGetter(row, ix)
        case "BOOLEAN" =>
          BooleanGetter(row, ix)
        case "TINYINT" =>
          ByteGetter(row, ix)
        case "SMALLINT" =>
          ShortGetter(row, ix)
        case "BIGINT" =>
          LongGetter(row, ix)
        case "DECIMAL" =>
          JavaBigDecimalGetter(row, ix)
        case "REAL" =>
          FloatGetter(row, ix)
        case "TIME" =>
          TimeGetter(row, ix)
        case "DATE" =>
          DateGetter(row, ix)
        case "TIMESTAMP" =>
          TimestampGetter(row, ix)
        case "BLOB" | "VARBINARY" =>
          ByteVectorGetter(row, ix)
        case "OTHER" =>
          SerializedGetter(row, ix)
        case "CHAR" | "VARCHAR" | "VARCHAR_IGNORECASE" =>
          StringGetter(row, ix)
        case "UUID" =>
          UUIDGetter(row, ix)
        case "ARRAY" =>
          def toArray(o: Any): ParameterValue = {
            o match {
              case a: Array[_] =>
                ParameterValue(a.map(elem => Option(elem).map(toArray)))
              case _ =>
                ParameterValue(o)
            }
          }
          Option(row.getObject(ix(row))).map(toArray)
      }
  }

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
        case b: Reader =>
          setParameter[Reader](preparedStatement, parameterIndex, b)
        case b: InputStream =>
          setParameter[InputStream](preparedStatement, parameterIndex, b)
        case b: UUID =>
          setParameter[UUID](preparedStatement, parameterIndex, b)
        case b: Serialized =>
          setParameter[Serialized](preparedStatement, parameterIndex, b)
        case b: QSeq =>
          setParameter[QSeq](preparedStatement, parameterIndex, b)
        case b: CompositeSetter[_] =>
          b.set(preparedStatement, parameterIndex, b)
      }
    }
  }

  /**
   * Class name for the DataSource class.
   */
  override def dataSourceClassName: String = "org.h2.jdbcx.JdbcDataSource"

  /**
   * Class name for the JDBC driver class.
   */
  override def driverClassName: String = "org.h2.Driver"

  //http://www.h2database.com/html/cheatSheet.html
  override def jdbcSchemes: Set[String] = {
    CISet(
      "h2",
      "h2:mem",
      "h2:tcp"
    )
  }

  /**
   * If the JDBC driver supports the .isValid() method.
   */
  override def supportsIsValid: Boolean = true

  /**
   * The result of getMetaData.getDatabaseProductName
   */
  override def productName: String = "H2"

  /**
   *
   * @param name The name of the database. A name is required if you want multiple connections or dbCloseDelay != Some(0).
   * @param dbCloseDelay The number of seconds to wait after the last connection closes before deleting the database. The default is right away, or Some(0). None means never.
   * @param f
   * @tparam T
   * @return
   */
  def withMemConnection[T](name: String = "", dbCloseDelay: Option[Int] = Some(0))(f: Connection => T): T = {
    val dbCloseDelayArg = s";DB_CLOSE_DELAY=${dbCloseDelay.getOrElse(-1)}"
    val connectionString = s"jdbc:h2:mem:$name$dbCloseDelayArg"
    val connection = DriverManager.getConnection(connectionString)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def withFileConnection[T](path: Path)(f: Connection => T): T = {
    val connection = DriverManager.getConnection("jdbc:h2:" + path.toFile.getCanonicalPath)

    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  override protected def toParameter(a: Any): Option[Any] = {
    a match {
      case null | None =>
        None
      case Some(a) =>
        Some(toParameter(a)).flatten
      case a =>
        Some(toH2Parameter(a))
    }
  }
}
