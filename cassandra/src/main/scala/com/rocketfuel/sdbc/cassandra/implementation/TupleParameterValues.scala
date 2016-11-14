package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core
import com.datastax.driver.core.{CodecRegistry, ProtocolVersion}
import shapeless._
import shapeless.ops.hlist.{Mapper, ToTraversable}
import shapeless.ops.product.ToHList
import shapeless.syntax.std.product._

private[sdbc] trait TupleParameterValues {
  self: Cassandra =>

  object ToTupleDataType extends Poly {

    implicit def fromValue[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: A) =>
          dt.dataType
      }
    }

  }

  object ToTupleDataValue extends Poly {

    implicit def fromValue[A](implicit dt: TupleDataType[A]) = {
      use {
        (value: A) => dt.toCassandraValue(value)
      }
    }

  }

  implicit def hlistTupleValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): TupleValue = {
    val dataTypes = dataTypeList(h.map(ToTupleDataType))
    val dataValueHList = h.map(ToTupleDataValue)
    val dataValues = dataValueList(dataValueHList)
    val underlying = core.TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, dataTypes: _*).newValue(dataValues: _*)
    TupleValue(underlying)
  }

  implicit def hlistParameterValue[
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](h: H
  )(implicit dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): ParameterValue = {
    hlistTupleValue(h)
  }

  implicit def productTupleValue[
    P <: Product,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: P
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef]
  ): TupleValue = {
    val asH = p.toHList
    val tv = hlistTupleValue(asH)
    tv
  }

  implicit def productParameterValue[
    P <: Product,
    H <: HList,
    ListH <: HList,
    MappedTypesH <: HList,
    MappedValuesH <: HList
  ](p: P
  )(implicit toHList: ToHList.Aux[P, H],
    dataTypeMapper: Mapper.Aux[ToTupleDataType.type, H, MappedTypesH],
    dataTypeList: ToTraversable.Aux[MappedTypesH, Seq, core.DataType],
    dataValueMapper: Mapper.Aux[ToTupleDataValue.type, H, MappedValuesH],
    dataValueList: ToTraversable.Aux[MappedValuesH, Seq, AnyRef],
    toParameterValue: TupleValue => ParameterValue
  ): ParameterValue = {
    val asTupleValue = productTupleValue(p)
    toParameterValue(asTupleValue)
  }

}
