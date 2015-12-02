package com.rocketfuel.sdbc.postgresql.implementation

import java.net.InetAddress
import java.sql._
import java.time._
import java.util.UUID
import com.rocketfuel.sdbc.base.jdbc._
import com.rocketfuel.sdbc.postgresql.{LTree, Cidr}
import com.rocketfuel.sdbc.postgresql.jdbc.LTree
import org.json4s._
import scala.reflect.runtime.universe._

private[sdbc] trait SeqParameter {

  def typeName[T](implicit tag: TypeTag[T]): String = {
    typeName(tag.tpe)
  }

  def typeName(tpe: Type): String = {
    tpe match {
      case t if t =:= typeOf[Short] => "int2"
      case t if t =:= typeOf[Int] => "int4"
      case t if t =:= typeOf[Long] => "int8"
      case t if t =:= typeOf[Float] => "float4"
      case t if t =:= typeOf[Double] => "float8"
      case t if t =:= typeOf[java.lang.Short] => "int2"
      case t if t =:= typeOf[java.lang.Integer] => "int4"
      case t if t =:= typeOf[java.lang.Long] => "int8"
      case t if t =:= typeOf[java.lang.Float] => "float4"
      case t if t =:= typeOf[java.lang.Double] => "float8"
      case t if t =:= typeOf[BigDecimal] => "numeric"
      case t if t =:= typeOf[java.math.BigDecimal] => "numeric"
      case t if t =:= typeOf[LocalDateTime] => "timestamp"
      case t if t =:= typeOf[OffsetDateTime] => "timestamptz"
      case t if t =:= typeOf[Date] => "date"
      case t if t =:= typeOf[Time] => "time"
      case t if t =:= typeOf[java.time.LocalDate] => "date"
      case t if t =:= typeOf[java.time.LocalTime] => "time"
      case t if t =:= typeOf[OffsetTime] => "timetz"
      case t if t =:= typeOf[Duration] => "interval"
      case t if t =:= typeOf[Boolean] => "boolean"
      case t if t =:= typeOf[java.lang.Boolean] => "boolean"
      case t if t =:= typeOf[String] => "text"
      case t if t =:= typeOf[scala.xml.Elem] => "xml"
      case t if t <:< typeOf[JValue] => "json"
      case t if t =:= typeOf[LTree] => "ltree"
      case t if t =:= typeOf[UUID] => "uuid"
      case t if t =:= typeOf[InetAddress] => "inet"
      case t if t =:= typeOf[Cidr] => "cidr"
      case t if t =:= typeOf[Map[String, String]] => "hstore"
      case t
        if t <:< typeOf[QSeq[_]]
          || t <:< typeOf[Seq[_]] =>

        innerTypeName(t)

      case t => throw new Exception("PostgreSQL does not understand " + t.toString)
    }
  }

  def innerTypeName(tpe: Type): String = {
    val innerType = tpe.dealias.typeArgs.head.dealias
    typeName(innerType)
  }

  implicit def SeqOptionToOptionParameterValue[T](
    v: Seq[Option[T]]
  )(implicit conversion: T => ParameterValue,
    ttag: TypeTag[T]
  ): ParameterValue = {
    ParameterValue(QSeq(v.map(_.map(conversion)), typeName[T]))
  }

  implicit def SeqToOptionParameterValue[T](
    v: Seq[T]
  )(implicit conversion: T => ParameterValue,
    ttag: TypeTag[T]
  ): ParameterValue = {
    v.map(Some.apply)
  }

  implicit val QSeqIsParameter: IsParameter[QSeq[_]] = new IsParameter[QSeq[_]] {
    override def set(preparedStatement: PreparedStatement, parameterIndex: Int, parameter: QSeq[_]): Unit = {
      preparedStatement.setArray(parameterIndex, parameter.asJdbcArray(preparedStatement.getConnection))
    }
  }

  implicit def SetterToSeqOptionUpdater[T](implicit conversion: T => ParameterValue, ttag: TypeTag[T]): Updater[Seq[Option[T]]] = {
    new Updater[Seq[Option[T]]] {
      override def update(row: UpdatableRow, columnIndex: Int, x: Seq[Option[T]]): Unit = {
        val asParameterValue: ParameterValue = x
        val ParameterValue(q: QSeq[_]) = asParameterValue
        row.updateArray(columnIndex, q.asJdbcArray(row.getStatement.getConnection))
      }
    }
  }

  implicit def SetterToSeqUpdater[T](implicit conversion: T => ParameterValue, ttag: TypeTag[T]): Updater[Seq[T]] = {
    new Updater[Seq[T]] {
      val optionUpdater = implicitly[Updater[Seq[Option[T]]]]
      override def update(row: UpdatableRow, columnIndex: Int, x: Seq[T]): Unit = {
        val optValue = x.map(Some.apply)
        optionUpdater.update(row, columnIndex, optValue)
      }
    }
  }

}
