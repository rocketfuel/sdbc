package com.rocketfuel.sdbc.postgresql.implementation

import com.rocketfuel.sdbc.base.jdbc.statement._
import java.sql.{Types, PreparedStatement}
import com.rocketfuel.sdbc.base.jdbc._
import org.postgresql.util.PGobject
import scala.collection.convert.decorateAsJava._

//PostgreSQL doesn't support Byte, so we don't use the default setters.
private[sdbc] trait Setters
  extends BooleanParameter
  with BytesParameter
  with DateParameter
  with BigDecimalParameter
  with DoubleParameter
  with FloatParameter
  with IntParameter
  with LongParameter
  with ShortParameter
  with StringParameter
  with TimestampParameter
  with ReaderParameter
  with InputStreamParameter
  with UUIDParameter
  with BlobParameter
  with OffsetDateTimeParameter
  with OffsetTimeParameter
  with InetAddressParameter
  with PGJsonParameter
  with LocalTimeParameter
  with PGobjectParameter
  with MapParameter
  with SeqWithXmlParameter {
  self: DBMS =>

}

private[sdbc] trait PGobjectParameter {
  self: ParameterValue =>

  implicit object PGobjectParameter extends Parameter[PGobject] {
    override val set: PGobject => (PreparedStatement, Int) => PreparedStatement = {
      value => (statement, parameterIndex) =>
        statement.setObject(parameterIndex + 1, value)
        statement
    }
  }

  implicit def isPGobjectParameter[A](value: A)(implicit toPGobject: A => PGobject): Parameter[A] = {
    new Parameter[A] {
      override val set: A => (PreparedStatement, Int) => PreparedStatement = {
        value => (statement, parameterIndex) =>
          val asPGobject = toPGobject(value)
          PGobjectParameter.set(asPGobject)(statement, parameterIndex)
      }
    }
  }

}

private[sdbc] trait MapParameter {
  self: ParameterValue =>

  implicit object MapParameter extends Parameter[Map[String, String]] {
    override val set: Map[String, String] => (PreparedStatement, Int) => PreparedStatement = {
      value => (statement, parameterIndex) =>
        statement.setObject(parameterIndex + 1, value.asJava)
        statement
    }
  }

}
