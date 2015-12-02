package com.rocketfuel.sdbc.sqlserver.implementation

import java.sql.PreparedStatement
import java.time.{LocalTime, OffsetDateTime}
import java.util.UUID

import com.rocketfuel.sdbc.base.{ToParameter, jdbc}
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.sqlserver.HierarchyId

import scala.xml.Node

private[sdbc] trait QLocalTimeImplicits {
  implicit val LocalTimeIsParameter: IsParameter[LocalTime] = new IsParameter[LocalTime] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: LocalTime): Unit = {
      preparedStatement.setString(parameterIndex, parameter.toString)
    }
  }

  implicit def LocalTimeToParameterValue(l: LocalTime): ParameterValue = {
    ParameterValue(l)
  }
}

private[sdbc] object QLocalTime extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case o: LocalTime => o
  }
}

private[sdbc] object QOffsetDateTime extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case o: OffsetDateTime => o
  }
}

private[sdbc] trait QOffsetDateTimeImplicits {
  implicit val OffsetDateTimeIsParameter: IsParameter[OffsetDateTime] = new IsParameter[OffsetDateTime] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: OffsetDateTime): Unit = {
      preparedStatement.setString(parameterIndex, offsetDateTimeFormatter.format(parameter))
    }
  }

  implicit def OffsetDateTimeToParameterValue(o: OffsetDateTime): ParameterValue = {
    ParameterValue(offsetDateTimeFormatter.format(o))
  }
}

private[sdbc] object QUUID extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case u: UUID => u
  }
}

private[sdbc] trait QUUIDImplicits {
  implicit val UUIDIsParameter: IsParameter[UUID] = new IsParameter[UUID] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: UUID): Unit = {
      preparedStatement.setString(parameterIndex, parameter.toString)
    }
  }

  implicit def UUIDToParameterValue(x: UUID): ParameterValue = {
    ParameterValue(x)
  }
}

private[sdbc] object QHierarchyId extends ToParameter {
  override val toParameter: PartialFunction[Any, Any] = {
    case h: HierarchyId => h
  }
}

private[sdbc] trait QHierarchyIdImplicits {
  implicit val HierarchyIdIsParameter: IsParameter[HierarchyId] = new IsParameter[HierarchyId] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: HierarchyId): Unit = {
      preparedStatement.setString(parameterIndex, parameter.toString)
    }
  }

  implicit def HierarchyIdToParameterValue(x: HierarchyId): ParameterValue = {
    ParameterValue(x)
  }
}

private[sdbc] object QXML extends ToParameter with QXMLImplicits {
  override val toParameter: PartialFunction[Any, Any] = {
    case x: Node => x
  }
}

private[sdbc] trait QXMLImplicits extends jdbc.QXMLImplicits {
  override implicit val NodeIsParameter: IsParameter[Node] = new IsParameter[Node] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: Node): Unit = {
      preparedStatement.setString(parameterIndex, parameter.toString)
    }
  }
}

//We have to use a special UUID getter, so we can't use the default setters.
private[sdbc] trait Setters
  extends QBooleanImplicits
  with QByteImplicits
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
  with QInstantImplicits
  with QLocalDateImplicits
  with QLocalTimeImplicits
  with QLocalDateTimeImplicits
  with QOffsetDateTimeImplicits
  with QUUIDImplicits
  with QHierarchyIdImplicits
  with QXMLImplicits {

  val toSqlServerParameter =
    QBoolean.toParameter orElse
      QByte.toParameter orElse
      QBytes.toParameter orElse
      QDate.toParameter orElse
      QBigDecimal.toParameter orElse
      QDouble.toParameter orElse
      QFloat.toParameter orElse
      QInt.toParameter orElse
      QLong.toParameter orElse
      QShort.toParameter orElse
      QString.toParameter orElse
      QTime.toParameter orElse
      QTimestamp.toParameter orElse
      QReader.toParameter orElse
      QInputStream.toParameter orElse
      QInstant.toParameter orElse
      QLocalDate.toParameter orElse
      QLocalTime.toParameter orElse
      QLocalDateTime.toParameter orElse
      QOffsetDateTime.toParameter orElse
      QUUID.toParameter orElse
      QHierarchyId.toParameter orElse
      QXML.toParameter

}
