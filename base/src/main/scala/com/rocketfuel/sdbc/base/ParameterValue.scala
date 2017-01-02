package com.rocketfuel.sdbc.base

import fs2.pipe
import fs2.util.Async
import scala.collection.GenMap
import shapeless._
import shapeless.record._
import shapeless.ops.record._

trait ParameterValue {

  type PreparedStatement

  protected def setNone(
    preparedStatement: PreparedStatement,
    parameterIndex: Int
  ): PreparedStatement

  protected def setOption[T](
    value: Option[T]
  )(implicit parameter: Parameter[T]
  ): (PreparedStatement, Int) => PreparedStatement = {
    value.map(parameter).getOrElse(setNone)
  }

  trait Parameter[-A] extends (A => (PreparedStatement, Int) => PreparedStatement)

  object Parameter {
    def apply[A](implicit parameter: Parameter[A]): Parameter[A] = parameter

    implicit def ofFunction1[A](set0: (A, PreparedStatement, Int) => PreparedStatement): Parameter[A] = new Parameter[A] {
      override def apply(v1: A): (PreparedStatement, Int) => PreparedStatement = {
        (preparedStatement: PreparedStatement, ix: Int) =>
          set0(v1, preparedStatement, ix)
      }
    }

    implicit def ofFunction2[A](set0: A => (PreparedStatement, Int) => PreparedStatement): Parameter[A] = new Parameter[A] {
      override def apply(v1: A): (PreparedStatement, Int) => PreparedStatement = set0(v1)
    }

    def derived[A, B](implicit convert: A => B, baseParameter: Parameter[B]): Parameter[A] = {
      converted[A, B](convert)
    }

    implicit def converted[A, B](convert: A => B)(implicit baseParameter: Parameter[B]): Parameter[A] = {
      (v1: A) => {
        val converted = convert(v1)
        (preparedStatement: PreparedStatement, ix: Int) =>
          baseParameter(converted)(preparedStatement, ix)
      }
    }

    def toString[A](implicit baseParameter: Parameter[String]): Parameter[A] = {
      converted[A, String](_.toString)
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
      ParameterValue(p, parameter(p.get))
    }

    implicit def of[T](p: T)(implicit parameter: Parameter[T]): ParameterValue = {
      Some(p)
    }

    //this is lazy to prevent a stack overflow on class loading.
    lazy val empty = ParameterValue(None, setNone)
  }

  type ParameterPositions = Map[String, Set[Int]]

  type Parameters = GenMap[String, ParameterValue]

  object Parameters {

    val empty: Parameters = Map.empty

    def isComplete(parameterValues: Parameters, parameterPositions: ParameterPositions): Boolean = {
      parameterValues.keySet == parameterPositions.keySet
    }

    class Products[
      A,
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](val genericA: LabelledGeneric.Aux[A, Repr],
      val valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      val toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    )

    object Products {
      implicit def apply[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit genericA: LabelledGeneric.Aux[A, Repr],
        valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): Products[A, Repr, Key, AsParameters] =
        new Products(genericA, valuesMapper, toMap)

      implicit def toGenericA[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit p: Products[A, Repr, Key, AsParameters]
      ): LabelledGeneric.Aux[A, Repr] =
        p.genericA

      implicit def toValuesMapper[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit p: Products[A, Repr, Key, AsParameters]
      ): MapValues.Aux[ToParameterValue.type, Repr, AsParameters] =
        p.valuesMapper

      implicit def toToMap[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit p: Products[A, Repr, Key, AsParameters]
      ): ToMap.Aux[AsParameters, Key, ParameterValue] =
        p.toMap
    }

    class Records[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](val valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
      val toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
    )

    object Records {
      implicit def apply[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit valuesMapper: MapValues.Aux[ToParameterValue.type, Repr, AsParameters],
        toMap: ToMap.Aux[AsParameters, Key, ParameterValue]
      ): Records[Repr, Key, AsParameters] =
        new Records(valuesMapper, toMap)

      implicit def toValuesMapper[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit r: Records[Repr, Key, AsParameters]
      ): MapValues.Aux[ToParameterValue.type, Repr, AsParameters] =
        r.valuesMapper

      implicit def toToMap[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit r: Records[Repr, Key, AsParameters]
      ): ToMap.Aux[AsParameters, Key, ParameterValue] =
        r.toMap

      implicit def ofProduct[
        A,
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit p: Products[A, Repr, Key, AsParameters]
      ): Records[Repr, Key, AsParameters] =
        Records[Repr, Key, AsParameters](p.valuesMapper, p.toMap)
    }

    def product[
      A,
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: A
    )(implicit p: Products[A, Repr, Key, AsParameters]
    ): Parameters = {
      val asGeneric = p.genericA.to(t)
      record(asGeneric)
    }

    def record[
      Repr <: HList,
      Key <: Symbol,
      AsParameters <: HList
    ](t: Repr
    )(implicit r: Records[Repr, Key, AsParameters]
    ): Parameters = {
      t.mapValues(ToParameterValue)(r.valuesMapper).toMap[Key, ParameterValue](r.toMap).map {
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
      ](implicit p: Products[A, Repr, Key, AsParameters]
      ): fs2.Pipe[F, A, Parameters] = {
        pipe.lift(product[A, Repr, Key, AsParameters])
      }

      def records[
        Repr <: HList,
        Key <: Symbol,
        AsParameters <: HList
      ](implicit r: Records[Repr, Key, AsParameters]
      ): fs2.Pipe[F, Repr, Parameters] = {
        pipe.lift(record[Repr, Key, AsParameters])
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
