package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}

trait StringContextMethods {
  self: DBMS =>

  implicit class JdbcStringContextMethods(sc: StringContext) {

    private def compiled = base.CompiledStatement(sc)

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
      ): Query[Unit] = {
        val parameterValues = toParameterValues(a)

        Query[Unit](compiled, parameterValues)
      }
    }

    object update extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Query[UpdateCount] = {
        val parameterValues = toParameterValues(a)

        Query[UpdateCount](compiled, parameterValues)
      }
    }

    object select extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Query[ImmutableRow] = {
        val parameterValues = toParameterValues(a)

        Query[ImmutableRow](compiled, parameterValues)
      }
    }

    object selectForUpdate extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Query[UpdatableRow] = {
        val parameterValues = toParameterValues(a)

        Query[UpdatableRow](compiled, parameterValues)
      }
    }

  }

}
