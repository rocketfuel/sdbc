package com.rocketfuel.sdbc.postgresql

import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.base.jdbc.statement._
import org.postgresql.util.PGobject
import scala.collection.JavaConverters._

//PostgreSQL doesn't support Byte, so we don't use the default setters.
trait Setters
  extends BooleanParameter
  with BytesParameter
  with PgDateParameter
  with BigDecimalParameter
  with DoubleParameter
  with FloatParameter
  with IntParameter
  with LongParameter
  with ShortParameter
  with StringParameter
  with ReaderParameter
  with InputStreamParameter
  with UUIDParameter
  with BlobParameter
  with OffsetDateTimeParameter
  with OffsetTimeParameter
  with InetAddressParameter
  with PGobjectParameter
  with HStoreParameter
  with SeqParameter
  with SQLXMLParameter {
  self: DBMS =>

}

trait PGobjectParameter {
  self: ParameterValue =>

  implicit val PGobjectParameter: Parameter[PGobject] =
    (value: PGobject, statement: PreparedStatement, ix: Int) => {
      statement.setObject(ix + 1, value)
      statement
    }

  implicit def isPGobjectParameter[A](implicit toPGobject: A => PGobject): Parameter[A] =
    Parameter.derived[A, PGobject]

}

trait HStoreParameter {
  self: ParameterValue =>

  implicit val HStoreJavaParameter: Parameter[java.util.Map[String, String]] =
    (value: java.util.Map[String, String], statement: PreparedStatement, ix: Int) => {
      statement.setObject(ix + 1, value)
      statement
    }

  implicit val HStoreScalaParameter: Parameter[Map[String, String]] =
    _.asJava

}
