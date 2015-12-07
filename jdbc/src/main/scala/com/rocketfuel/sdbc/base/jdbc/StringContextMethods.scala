package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.CompiledStatement
import shapeless._

trait StringContextMethods {
  self: DBMS =>

  implicit class JdbcStringContextMethods(sc: StringContext) {

    private def compiled = CompiledStatement(sc)

    object batch extends ProductArgs {
      def applyProduct[A <: HList](a: A)(implicit setter: CompositeSetter[A]): Batch = {
        val parameterValues = setter(a)

        Batch(compiled, Map.empty, Seq.empty).addBatch(parameterValues: _*)
      }
    }

    object execute extends ProductArgs {
      def applyProduct[A <: HList](a: A)(implicit setter: CompositeSetter[A]): Execute = {
        val parameterValues = setter(a)

        Execute(compiled, Map.empty[String, Option[Any]]).on(parameterValues: _*)
      }
    }

    object update extends ProductArgs {
      def applyProduct[A <: HList](a: A)(implicit setter: CompositeSetter[A]): Update = {
        val parameterValues = setter(a)

        Update(compiled, Map.empty[String, Option[Any]]).on(parameterValues: _*)
      }
    }

    object select extends ProductArgs {
      def applyProduct[A <: HList](a: A)(implicit setter: CompositeSetter[A]): Select[ImmutableRow] = {
        val parameterValues = setter(a)

        Select[ImmutableRow](compiled, Map.empty[String, Option[Any]]).on(parameterValues: _*)
      }
    }

    object selectForUpdate extends ProductArgs {
      def applyProduct[A <: HList](a: A)(implicit setter: CompositeSetter[A]): SelectForUpdate = {
        val parameterValues = setter(a)

        SelectForUpdate(compiled, Map.empty[String, Option[Any]]).on(parameterValues: _*)
      }
    }

  }

}
