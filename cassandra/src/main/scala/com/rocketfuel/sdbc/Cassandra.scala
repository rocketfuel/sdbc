package com.rocketfuel.sdbc

import com.datastax.driver.core
import com.rocketfuel.sdbc.cassandra.{ParameterValue, RowConverter, ToSessionMethods}
import com.rocketfuel.sdbc.base.CompiledStatement
import shapeless.ops.hlist._
import shapeless.{HList, ProductArgs}

/**
  * Import the contents of this object to interact with [[http://cassandra.apache.org/ Apache Cassandra]] using the Datastax driver.
  *
  * {{{
  * import com.rocketfuel.sdbc.Cassandra._
  *
  * val session = cluster.connect()
  *
  * session.iterator[Int]("SELECT 1")
  * }}}
  */
object Cassandra
  extends ParameterValue
  with ToSessionMethods {

  type ResultSet = core.ResultSet

  type Session = core.Session

  type Cluster = core.Cluster

  type UDTValue = core.UDTValue

  type Token = core.Token

  type CompositeGetter[A] = cassandra.CompositeGetter[A]
  val CompositeGetter = cassandra.CompositeGetter

  type CompositeTupleGetter[A] = cassandra.CompositeTupleGetter[A]
  val CompositeTupleGetter = cassandra.CompositeTupleGetter

  type Query[A] = cassandra.Query[A]
  val Query = cassandra.Query

  type Queryable[Key, Value] = cassandra.Queryable[Key, Value]
  val Queryable = cassandra.Queryable

  type QueryOptions = cassandra.QueryOptions
  val QueryOptions = cassandra.QueryOptions

  type RowConverter[A] = cassandra.RowConverter[A]
  val RowConverter = cassandra.RowConverter

  type TupleDataType[A] = cassandra.TupleDataType[A]
  val TupleDataType = cassandra.TupleDataType

  type TupleGetter[+A] = cassandra.TupleGetter[A]
  val TupleGetter = cassandra.TupleGetter

  type TupleValue = cassandra.TupleValue
  val TupleValue = cassandra.TupleValue

  implicit class CassandraStringContextMethods(sc: StringContext) {

    private val compiled = CompiledStatement(sc)

    private def toParameterValues[
      A <: HList,
      MappedA <: HList
    ](a: A
    )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
      toList: ToList[MappedA, ParameterValue]
    ): Parameters = {
      a
      .map(ToParameterValue)
      .toList
      .zipWithIndex
      .map {
        case (parameter, ix) =>
          (ix.toString, parameter)
      } toMap
    }

    object query extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList,
        B
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue],
        rowConverter: RowConverter[B]
      ): Query[B] = {
        val parameterValues = toParameterValues(a)

        Query(compiled).onParameters(parameterValues)
      }
    }

  }

}
