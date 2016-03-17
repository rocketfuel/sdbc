package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base.CompiledStatement
import com.rocketfuel.sdbc.cassandra.QueryOptions
import scalaz.concurrent.Task
import shapeless.{ProductArgs, HList}
import shapeless.ops.hlist._

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
        } toSeq
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

        Query(compiled, QueryOptions.default, Map.empty[String, ParameterValue]).on(parameterValues)
      }
    }

  }

}
