package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.nio.ByteBuffer
import com.rocketfuel.sdbc.base.CompiledStatement
import com.rocketfuel.sdbc.cassandra.{Execute, Select, QueryOptions}
import scodec.bits.ByteVector

private[sdbc] trait StringContextMethods {
  self: Cassandra =>

  implicit class CassandraStringContextMethods(sc: StringContext) {
    private def byNumberName(args: Seq[Any]): Map[String, Option[Any]] = {
      val argNames = 0.until(sc.parts.count(_.isEmpty)).map(_.toString)
      val parameters = argNames.zip(args.map(toParameter)).toMap
      parameters
    }

    private val compiled = CompiledStatement(sc)

    def execute(args: Any*): Execute = {
      Execute(compiled, byNumberName(args), QueryOptions.default)
    }

    def select(args: Any*): Select[Row] = {
      Select[Row](compiled, byNumberName(args), QueryOptions.default)
    }
  }

  private def toParameterNonOption(a: Any): Any = {
    a match {
      case b: Boolean => b
      case b: java.lang.Boolean => b.booleanValue
      case b: ByteVector => b
      case b: ByteBuffer => ByteVector(b)
      case a: Array[Byte] => ByteVector(a)
      case d: java.util.Date => d
      case d: java.math.BigDecimal => d
      case d: BigDecimal => d.underlying()
      case d: Double => d
      case d: java.lang.Double => d.doubleValue()
      case f: Float => f
      case f: java.lang.Float => f.floatValue()
      case i: InetAddress => i
      case i: Int => i
      case i: java.lang.Integer => i.intValue()
      case l: java.util.List[_] => l
      case l: Long => l
      case l: java.lang.Long => l.longValue()
      case m: java.util.Map[_, _] => m
      case s: java.util.Set[_] => s
      case s: String => s
      case u: java.util.UUID => u
      case t: Token => t
      case t: TupleValue => t
      case u: UDTValue => u
      case b: java.math.BigInteger => b
    }
  }

  /**
   * Currently Scala tuples are not supported.
   * @param a
   * @return
   */
  private def toParameter(a: Any): Option[Any] = {
    a match {
      case null | None =>
        None
      case Some(a) =>
        Some(toParameter(a)).flatten
      case _ =>
        Some(toParameterNonOption(a))
    }
  }

}
