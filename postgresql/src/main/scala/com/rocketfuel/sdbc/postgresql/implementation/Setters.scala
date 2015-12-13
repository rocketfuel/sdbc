package com.rocketfuel.sdbc.postgresql.implementation

import java.sql.PreparedStatement

import com.rocketfuel.sdbc.base.{ParameterValue, ToParameter}
import com.rocketfuel.sdbc.base.jdbc._
import org.postgresql.util.PGobject
import scala.collection.convert.decorateAsJava._

//PostgreSQL doesn't support Byte, so we don't use the default setters.
private[sdbc] trait Setters
  extends QPGObjectImplicits
  with QBooleanImplicits
  with QBytesImplicits
  with QDateImplicits
  with QBigDecimalImplicits
  with QDoubleImplicits
  with QFloatImplicits
  with QIntImplicits
  with QLongImplicits
  with QShortImplicits
  with QStringImplicits
  with QTimeImplicits
  with QTimestampImplicits
  with QReaderImplicits
  with QInputStreamImplicits
  with QUUIDImplicits
  with QInstantImplicits
  with QLocalDateImplicits
  with PGLocalTimeImplicits
  with QLocalDateTimeImplicits
  with PGTimeTzImplicits
  with PGTimestampTzImplicits
  with PGInetAddressImplicits
  with QXMLImplicits
  with QSQLXMLImplicits
  with QBlobImplicits
  with PGJsonImplicits
  with QMapImplicits {


}

private[sdbc] object QPGObject extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: PGobject => i
  }
}

private[sdbc] trait QPGObjectImplicits {
  implicit val PGobjectIsParameter: IsParameter[PGobject] = new IsParameter[PGobject] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: PGobject): Unit = {
      preparedStatement.setObject(parameterIndex, parameter)
    }
  }

  implicit def PGobjectToParameterValue(value: PGobject): ParameterValue = {
    ParameterValue(value)
  }

  implicit def IsPGobjectToParameterValue[T](value: T)(implicit converter: T => PGobject): ParameterValue = {
    converter(value)
  }

}

private[sdbc] object QMap extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case i: Map[_, _] => //Technically, this should be a Map[String, String]
      i.asJava
  }
}

private[sdbc] trait QMapImplicits {
  implicit val MapIsParameter: IsParameter[java.util.Map[String, String]] = new IsParameter[java.util.Map[String, String]] {
    override def set(
      preparedStatement: PreparedStatement,
      parameterIndex: Int,
      parameter: java.util.Map[String, String]
    ): Unit = {
      preparedStatement.setObject(parameterIndex, parameter)
    }
  }

  implicit def MapStringStringToParameterValue(value: Map[String, String]): ParameterValue = {
    ParameterValue(value.asJava)
  }
}
