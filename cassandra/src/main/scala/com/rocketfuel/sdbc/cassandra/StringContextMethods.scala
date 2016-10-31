package com.rocketfuel.sdbc.cassandra

import com.rocketfuel.sdbc.base.CompiledStatement
import shapeless.ops.hlist._
import shapeless.{HList, ProductArgs}

private[sdbc] trait StringContextMethods {
  self: Cassandra =>

  implicit class CassandraStringContextMethods(sc: StringContext) {

    private val compiled = CompiledStatement(sc)

    private def toParameterValues[
      A <: HList,
      MappedA <: HList
    ](a: A
    )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
      toList: ToList[MappedA, ParameterValue]
    ): Parameters = {
      a.
        map(ToParameterValue).
        toList.
        zipWithIndex.
        map {
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
