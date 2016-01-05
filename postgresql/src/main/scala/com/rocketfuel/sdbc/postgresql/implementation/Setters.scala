package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.PreparedStatement
import com.rocketfuel.sdbc.base.jdbc._
import org.postgresql.util.PGobject

//PostgreSQL doesn't support Byte, so we don't use the default setters.
private[sdbc] trait Setters
  extends PGobjectParameter
  with BooleanParameter
  with BytesParameter
  with DateParameter
  with BigDecimalParameter
  with DoubleParameter
  with FloatParameter
  with IntParameter
  with LongParameter
  with ShortParameter
  with StringParameter
  with TimeParameter
  with TimestampParameter
  with ReaderParameter
  with InputStreamParameter
  with UUIDParameter
  with InstantParameter
  with LocalDateParameter
  with LocalTimeParameter
  with LocalDateTimeParameter
  with PGTimeTzImplicits
  with PGTimestampTzImplicits
  with PGInetAddressImplicits
  with XMLParameter
  with SQLXMLParameter
  with BlobParameter
  with PGJsonImplicits
  with MapParameter {

}

private[sdbc] trait PGobjectParameter {
  self: ParameterValue =>

  implicit object PGobjectParameter extends Parameter[PGobject] {
    override val set: (PGobject) => (Statement, Int) => Statement = {
      value => (statement, parameterIndex) =>
        statement.setObject(parameterIndex + 1, value)
        statement
    }
  }

  implicit def isPGobjectParameter[A](value: A)(implicit toPGobject: A => PGobject): Parameter[A] = {
    new Parameter[A] {
      override val set: (A) => (PreparedStatement, Int) => PreparedStatement = {
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
    override val set: (PGobject) => (Statement, Int) => Statement = {
      value => (statement, parameterIndex) =>
        statement.setObject(parameterIndex + 1, value)
        statement
    }
  }

}
