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

    object batch extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): Batch = {
        val parameterValues = toParameterValues(a)

        Batch(compiled, Map.empty, Seq.empty).addBatch(parameterValues: _*)
      }
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

        Execute(compiled, Map.empty[String, ParameterValue]).on(parameterValues: _*)
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

        Update(compiled, Map.empty[String, ParameterValue]).on(parameterValues: _*)
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

        Query[ImmutableRow](compiled, Map.empty[String, ParameterValue]).on(parameterValues: _*)
      }
    }

    object selectForUpdate extends ProductArgs {
      def applyProduct[
        A <: HList,
        MappedA <: HList
      ](a: A
      )(implicit mapper: Mapper.Aux[ToParameterValue.type, A, MappedA],
        toList: ToList[MappedA, ParameterValue]
      ): SelectForUpdate = {
        val parameterValues = toParameterValues(a)

        SelectForUpdate(compiled, Map.empty[String, ParameterValue]).on(parameterValues: _*)
      }
    }

  }

}
