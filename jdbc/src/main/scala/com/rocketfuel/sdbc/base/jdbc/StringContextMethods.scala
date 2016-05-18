package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base
import java.io.Closeable
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
      ): Select[Unit] = {
        val parameterValues = toParameterValues(a)

        Select[Unit](compiled, parameterValues)
      }
    }

    object update extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Select[UpdateCount] = {
        val parameterValues = toParameterValues(a)

        Select[UpdateCount](compiled, parameterValues)
      }
    }

    object select extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Select[CloseableIterator[ImmutableRow]] = {
        val parameterValues = toParameterValues(a)

        Select[CloseableIterator[ImmutableRow] with Closeable](compiled, parameterValues)
      }
    }

    object selectForUpdate extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Select[CloseableIterator[UpdatableRow]] = {
        val parameterValues = toParameterValues(a)

        Select[CloseableIterator[UpdatableRow]](compiled, parameterValues)
      }
    }

  }

}
