package com.rocketfuel.sdbc.cassandra.implementation

import com.rocketfuel.sdbc.base.CompiledStatement
import com.rocketfuel.sdbc.cassandra.QueryOptions
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
    ): ParameterList = {
      a.
        map(ToParameterValue).
        toList.
        zipWithIndex.
        map {
          case (parameter, ix) =>
            (ix.toString, parameter)
        } toSeq
    }

    object execute extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Execute = {
        val parameterValues = toParameterValues(a)

        Execute(compiled, Map.empty[String, ParameterValue], QueryOptions.default).on(parameterValues: _*)
      }
    }

    object select extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Select[Row] = {
        val parameterValues = toParameterValues(a)

        Select[Row](compiled, Map.empty[String, ParameterValue], QueryOptions.default).on(parameterValues: _*)
      }
    }

  }

}
