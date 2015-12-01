package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.record._
import shapeless.ops.record._
import shapeless.ops.hlist._

trait CompositeSetter[A] extends (A => Seq[(String, Option[ParameterValue])])

object CompositeSetter {

  def apply[A](implicit compositeA: CompositeSetter[A]): CompositeSetter[A] = compositeA

  implicit def fromSetter[
    Key <: Symbol,
    A
  ](implicit converter: A => Option[ParameterValue],
    key: Key
  ): CompositeSetter[A] =
    new CompositeSetter[A] {
      override def apply(v1: A): Seq[(String, Option[ParameterValue])] = {
        Seq((key.name, converter(v1)))
      }
    }

  object ToParameterValue extends FieldPoly {
    implicit def valueToParameter[A](implicit converter: A => Option[ParameterValue]) = {
      use((parameter: A) => converter(parameter))
    }
  }

  def fromGeneric[
    A,
    Repr <: HList,
    MappedRepr <: HList,
    ReprKeys <: HList
  ](implicit genericA: LabelledGeneric.Aux[A, Repr],
    mapper: Mapper.Aux[ToParameterValue.type, Repr, MappedRepr],
    keys: Keys.Aux[Repr, ReprKeys],
    ktl: ToList[ReprKeys, Symbol],
    vtl: ToList[MappedRepr, Option[ParameterValue]]
  ): CompositeSetter[A] = {
    new CompositeSetter[A] {
      val aFromRecord = fromRecord[Repr, MappedRepr, ReprKeys]
      override def apply(v1: A): Seq[(String, Option[ParameterValue])] = {
        val asGeneric = genericA.to(v1)
        aFromRecord(asGeneric)
      }
    }

  }

  def fromRecord[
    Repr <: HList,
    MappedRepr <: HList,
    ReprKeys <: HList
  ](implicit mapper: Mapper.Aux[ToParameterValue.type, Repr, MappedRepr],
    keys: Keys.Aux[Repr, ReprKeys],
    ktl: ToList[ReprKeys, Symbol],
    vtl: ToList[MappedRepr, Option[ParameterValue]]
  ): CompositeSetter[Repr] = {
    new CompositeSetter[Repr] {
      override def apply(v1: Repr): Seq[(String, Option[ParameterValue])] = {
        val mapped = v1.map(ToParameterValue)
        v1.keys.toList.map(_.name) zip mapped.toList
      }
    }
  }

}
