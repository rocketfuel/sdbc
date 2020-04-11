package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.Logger
import java.io.InputStream
import java.net.URL
import java.nio.file.Path
import scala.reflect.ClassTag

trait QueryCompanion {
  self: DBMS with Connection =>

  trait QueryCompanion[Query] extends Logger {

    protected def ofCompiledStatement(statement: CompiledStatement): Query

    def readInputStream(
      stream: InputStream,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): Query = {
      ofCompiledStatement(CompiledStatement.readInputStream(stream, hasParameters))
    }

    def readUrl(
      u: URL,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): Query = {
      ofCompiledStatement(CompiledStatement.readUrl(u, hasParameters))
    }

    def readPath(
      path: Path,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): Query = {
      ofCompiledStatement(CompiledStatement.readPath(path, hasParameters))
    }

    def readClassResource(
      clazz: Class[_],
      name: String,
      nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): Query = {
      ofCompiledStatement(CompiledStatement.readClassResource(clazz, name, nameMangler, hasParameters))
    }

    def readTypeResource[A](
      name: String,
      nameMangler: (Class[_], String) => String = CompiledStatement.NameManglers.default,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default,
      tag: ClassTag[A]
    ): Query = {
      ofCompiledStatement(CompiledStatement.readTypeResource(name, nameMangler, hasParameters))
    }

    def readResource(
      name: String,
      hasParameters: Boolean = true
    )(implicit codec: scala.io.Codec = scala.io.Codec.default
    ): Query = {
      ofCompiledStatement(CompiledStatement.readResource(name, hasParameters))
    }

    protected def logRun(
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      QueryCompanion.logRun(log, compiledStatement, parameters)
    }

  }

  object QueryCompanion {
    def logRun(
      logger: com.typesafe.scalalogging.Logger,
      compiledStatement: CompiledStatement,
      parameters: Parameters
    ): Unit = {
      logger.debug(s"""query "${compiledStatement.originalQueryText}", parameters $parameters""")
    }
  }

}
