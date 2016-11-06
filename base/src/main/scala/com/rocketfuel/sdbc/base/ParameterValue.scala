package com.rocketfuel.sdbc.base

import fs2.pipe
import fs2.util.Async
import shapeless._
import shapeless.record._
import shapeless.ops.record._

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
      s"ParameterValue($value)"
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

    //this is lazy to prevent a stack overflow on class loading.
    lazy val empty = ParameterValue(None, setNone)
  }

  type Parameters = Map[String, ParameterValue]

  object Parameters {

    val empty: Parameters = Map.empty

    def product[
      A,
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: A
    )(implicit genericA: LabelledGeneric.Aux[A, Repr],
      valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    ): Parameters = {
      val asGeneric = genericA.to(t)
      record(asGeneric)
    }

    def record[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: Repr
    )(implicit valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    ): Parameters = {
      t.mapValues(ToParameterValue).toMap[Key, ParameterValue].map {
        case (symbol, value) => symbol.name -> value
      }
    }

    case class Pipe[F[_]](implicit async: Async[F]) {
      def combine(p: Parameters): fs2.Pipe[F, Parameters, Parameters] = {
        pipe.lift(p ++ _)
      }

      def products[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit genericA: LabelledGeneric.Aux[A, Repr],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, A, Parameters] = {
        pipe.lift(Parameters.product[A, Repr, Key, AsParameters])
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): fs2.Pipe[F, Repr, Parameters] = {
        pipe.lift(Parameters.record[Repr, Key, AsParameters])
      }
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
}
