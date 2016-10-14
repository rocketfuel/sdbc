package com.rocketfuel.sdbc.base

import shapeless._
import shapeless.record._
import shapeless.ops.record._
import shapeless.ops.hlist._

trait ParameterValue {

  type PreparedStatement

  type Connection

  protected def setNone(
    preparedStatement: PreparedStatement,
    parameterIndex: Int
  ): PreparedStatement

  protected def setOption[T](
    value: Option[T]
  )(implicit parameter: Parameter[T]
  ): (PreparedStatement, Int) => PreparedStatement = {
    value.map(parameter.set).getOrElse(setNone)
  }

  trait Parameter[-A] {
    val set: A => (PreparedStatement, Int) => PreparedStatement
  }

  object Parameter {
    def apply[A](implicit parameter: Parameter[A]): Parameter[A] = parameter

    implicit def ofFunction[A](set0: A => (PreparedStatement, Int) => PreparedStatement): Parameter[A] = new Parameter[A] {
      override val set: (A) => (PreparedStatement, Int) => PreparedStatement = set0
    }
  }

  trait DerivedParameter[-A] extends Parameter[A] {

    type B

    val conversion: A => B
    val baseParameter: Parameter[B]

    override val set: A => (PreparedStatement, Int) => PreparedStatement = {
      (value: A) => {
        val converted = conversion(value)
        (statement, parameterIndex) =>
          baseParameter.set(converted)(statement, parameterIndex)
      }
    }

  }

  object DerivedParameter {
    type Aux[A, B0] = DerivedParameter[A] { type B = B0 }

    implicit def apply[A, B0](implicit conversion0: A => B0, baseParameter0: Parameter[B0]): DerivedParameter[A] =
      new DerivedParameter[A] {
        type B = B0
        override val conversion: A => B = conversion0
        override val baseParameter: Parameter[B] = baseParameter0
      }

  }

  case class ParameterValue private[sdbc] (
    value: Option[Any],
    set: (PreparedStatement, Int) => PreparedStatement
  ) {
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case ParameterValue(otherValue, _) =>
          otherValue == value
        case otherwise =>
          false
      }
    }

    override def hashCode(): Int = {
      value.hashCode()
    }

    override def toString: String = {
      value.toString
    }
  }

  object ParameterValue {
    def apply[T](p: T)(implicit toParameterValue: T => ParameterValue): ParameterValue = {
      p
    }

    implicit def ofOption[T](p: Option[T])(implicit parameter: Parameter[T]): ParameterValue = {
      p match {
        case s: Some[T] =>
          ParameterValue(p, setOption(p))
        case None =>
          ofNone(None)
      }
    }

    implicit def ofNone(p: None.type): ParameterValue = {
      empty
    }

    implicit def ofSome[T](p: Some[T])(implicit parameter: Parameter[T]): ParameterValue = {
      ParameterValue(p, parameter.set(p.get))
    }

    implicit def of[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      Some(p)
    }

    lazy val empty = ParameterValue(None, setNone)
  }

  type Parameters = Map[String, ParameterValue]

  object Parameters {

    val empty: Parameters = Map.empty

    def product[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Parameters = {
      val asGeneric = genericA.to(t)
      record(asGeneric)
    }

    def record[
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Parameters = {
      val mapped = t.values.map(ToParameterValue)
      t.keys.toList.map(_.name).zip(mapped.toList).toMap
    }

  }

  object ToParameterValue extends Poly {
    implicit def fromValue[A](implicit parameter: Parameter[A]) = {
      use {
        (value: A) =>
          ParameterValue.of[A](value)
      }
    }

    implicit def fromOptionalValue[A](implicit parameter: Parameter[A]) = {
      use {
        (value: Option[A]) =>
          ParameterValue.ofOption[A](value)
      }
    }

    implicit def fromSomeValue[A](implicit parameter: Parameter[A]) = {
      use {
        (value: Some[A]) =>
          ParameterValue.of[A](value.get)
      }
    }

    implicit def fromNone(implicit parameter: Parameter[None.type]) = {
      use {
        (value: None.type) =>
          ParameterValue.ofNone(value)
      }
    }
  }

  type ParameterBatches = Seq[Parameters]

  object ParameterBatches {

    val empty: ParameterBatches = Seq.empty

    implicit def products[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](ts: Seq[A]
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): ParameterBatches = {
      val asGeneric = ts.map(genericA.to)
      records(asGeneric)
    }

    implicit def records[
      Repr <: HList,
      ReprKeys <: HList,
      ReprValues <: HList,
      MappedRepr <: HList
    ](ts: Seq[Repr]
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      values: Values.Aux[Repr, ReprValues],
      valuesMapper: Mapper.Aux[ToParameterValue.type, ReprValues, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): ParameterBatches = {
      ts.map(Parameters.record(_))
    }

  }

}
