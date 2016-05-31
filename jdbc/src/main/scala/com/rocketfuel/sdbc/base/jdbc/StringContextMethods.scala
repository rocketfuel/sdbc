package com.rocketfuel.sdbc.base.jdbc

import java.io.Closeable
import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}

trait StringContextMethods {
  self: DBMS =>

  implicit class JdbcStringContextMethods(sc: StringContext) {

    private def compiled = CompiledStatement(sc)

    private def toParameterValues[
      A <: HList,
      MappedA <: HList
    ](a: A
    )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
      toList: ToList[MappedA, ParameterValue]
    ): Map[String, ParameterValue] = {
      a.
        map(ToParameterValue).
        toList.
        zipWithIndex.
        map {
          case (parameter, ix) =>
            (ix.toString, parameter)
        } toMap
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

        Execute(compiled, parameterValues)
      }
    }

    object update extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Update = {
        val parameterValues = toParameterValues(a)

        Update(compiled, parameterValues)
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

        Select[Row](compiled, parameterValues)
      }
    }

    object selectForUpdate extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): SelectForUpdate[UpdatableRow] = {
        val parameterValues = toParameterValues(a)

        SelectForUpdate[UpdatableRow](compiled, parameterValues)
      }
    }

  }

}
