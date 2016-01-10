package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base.box
import com.datastax.driver.core
import shapeless._
import shapeless.ops.hlist.{ToList, Mapper}
import shapeless.ops.product._
import shapeless.syntax.std.product._

private[sdbc] trait TupleParameterValues {
  self: Cassandra =>

  object ToTupleDataType extends Poly {

    implicit def fromValue[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: A) =>
          dt
      }
    }

    implicit def fromOptionalValue[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: Option[A]) =>
          dt
      }
    }

    implicit def fromSomeValue[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: Some[A]) =>
          dt

      }
    }

    val noneDataType = TupleDataType(core.DataType.custom(classOf[java.lang.Object].getName), Function.const(null))

    implicit val none = use {
      (value: None.type) =>
        noneDataType
    }

  }

  object ToTupleDataValue extends Poly {

    implicit def fromValue[A](a: A)(implicit dt: TupleDataType[A]) = {
      use {
        (value: A) => dt.toCassandraValue(value)
      }
    }

    implicit def fromOption[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: Option[A]) => value.map(dt.toCassandraValue andThen box).orNull
      }
    }

    implicit def fromSome[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: Some[A]) => dt.toCassandraValue(value.get)
      }
    }

    implicit def fromNone = {
      use {
        (value: None.type) => null
      }
    }

  }

  implicit def hlistParameterValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToList[MappedTypesH, TupleDataType[Any]],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToList[MappedValuesH, Any]
  ): TupleValue = {
    val dataTypes = h.map(ToTupleDataType).toList.map(_.dataType)
    val dataValues = h.map(ToTupleDataValue).toList
    val underlying = core.TupleType.of(dataTypes: _*).newValue(dataValues: _*)
    TupleValue(underlying)
  }

  implicit def productParameterValue[
    P <: Product,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: Product
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToList[MappedTypesH, TupleDataType[Any]],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToList[MappedValuesH, Any]
  ): TupleValue = {
    hlistParameterValue(p.toHList)
  }

}
