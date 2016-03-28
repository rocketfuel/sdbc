package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.tuple

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
      ): Query[Unit, Result.Unit] = {
        val parameterValues = toParameterValues(a)

        new Query[Unit, Result.Unit](compiled, parameterValues)
      }
    }

    object update extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Query[Long, Result.UpdateCount] = {
        val parameterValues = toParameterValues(a)

        new Query[Long, Result.UpdateCount](compiled, parameterValues)
      }
    }

    object query extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Query[Iterator[ImmutableRow], Result.ImmutableIterator] = {
        val parameterValues = toParameterValues(a)

        new Query[Iterator[ImmutableRow], Result.ImmutableIterator](compiled, parameterValues)(StatementConverter.immutableIterator)
      }
    }

  }

}
