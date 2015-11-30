package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.record._
import shapeless.labelled.FieldType
import shapeless.ops.record._
import shapeless.ops.hlist._

case class CompositeParameter(parameters: Seq[(String, Option[ParameterValue])])

object CompositeParameter {
  implicit def from[A](a: A)(implicit compositeSetter: CompositeSetter[A]): CompositeParameter = {
    CompositeParameter(compositeSetter(a))
  }
}

trait CompositeSetter[A] extends (A => Seq[(String, Option[ParameterValue])])

object CompositeSetter {

  def apply[A](implicit compositeA: CompositeSetter[A]): CompositeSetter[A] = compositeA

  implicit def fromHNil[Tail <: HNil] =
    new CompositeSetter[Tail] {
      override def apply(v1: Tail): Seq[(String, Option[ParameterValue])] = {
        Seq.empty
      }
    }

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

  implicit def fromGeneric[
    A,
    Repr <: HList,
    MappedRepr <: HList,
    AKeys <: HList
  ](implicit genericA: LabelledGeneric.Aux[A, Repr],
    mapper: Mapper.Aux[ToParameterValue.type, Repr, MappedRepr],
    keys: Keys.Aux[Repr, AKeys],
    ktl: ToList[AKeys, Symbol],
    vtl: ToList[MappedRepr, Option[ParameterValue]]
  ): CompositeSetter[A] = {
    new CompositeSetter[A] {
      override def apply(v1: A): Seq[(String, Option[ParameterValue])] = {
        val asGeneric = genericA.to(v1)
        val mapped = asGeneric.map(ToParameterValue)
        asGeneric.keys.toList.map(_.name) zip mapped.toList
      }
    }

  }

  implicit def fromRecord[
    Repr <: HList,
    MappedRepr <: HList,
    Keys <: HList,
    MappedReprWithKeys <: HList
  ](implicit mapper: MapValues.Aux[CompositeSetter.ToParameterValue.type, Repr, MappedRepr],
    withKeys: ZipWithKeys.Aux[Keys, MappedRepr, MappedReprWithKeys],
    vtl: ToList[MappedReprWithKeys, (Symbol, Option[ParameterValue])]
  ): CompositeSetter[Repr] = {
    new CompositeSetter[Repr] {
      override def apply(v1: Repr): Seq[(String, Option[ParameterValue])] = {
        val mapped = v1.mapValues(ToParameterValue)
        mapped.zipWithKeys(withKeys).toList.map { case (key, value) => (key.name, value) }
      }
    }

  }

}
