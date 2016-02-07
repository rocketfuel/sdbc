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
    implicit def apply[A](set0: A => (PreparedStatement, Int) => PreparedStatement): Parameter[A] = new Parameter[A] {
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
      s"ParameterValue($value)"
    }
  }

  object ParameterValue {
    def apply[T](p: T)(implicit toParameterValue: T => ParameterValue): ParameterValue = {
      p
    }

    implicit def ofOption[T](p: Option[T])(implicit parameter: Parameter[T]): ParameterValue = {
      ParameterValue(p, setOption(p))
    }

    implicit def of[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      ofOption[T](Some(p))
    }

    implicit def ofNone(p: None.type): ParameterValue = {
      empty
    }

    val empty = ParameterValue(None, setNone)
  }

  case class Parameters(parameters: Map[String, ParameterValue])

  object Parameters {
    val empty = Parameters(Map.empty[String, ParameterValue])

    def apply(parameters: (String, ParameterValue)*): Parameters = {
      parameters
    }

    implicit def map(parameters: Map[String, ParameterValue]): Parameters = {
      Parameters(parameters)
    }

    implicit def seq(parameters: Seq[(String, ParameterValue)]): Parameters = {
      Parameters(Map(parameters: _*))
    }

    implicit def product[
      A,
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Parameters = {
      val asGeneric = genericA.to(t)
      record(asGeneric)
    }

    implicit def record[
      Repr <: HList,
      ReprKeys <: HList,
      MappedRepr <: HList
    ](t: Repr
    )(implicit keys: Keys.Aux[Repr, ReprKeys],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, MappedRepr],
      ktl: ToList[ReprKeys, Symbol],
      vtl: ToList[MappedRepr, ParameterValue]
    ): Parameters = {
      val mapped = t.mapValues(ToParameterValue)
      t.keys.toList.map(_.name) zip mapped.toList
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


}
