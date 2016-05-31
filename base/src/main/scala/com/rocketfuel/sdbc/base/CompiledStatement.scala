package com.rocketfuel.sdbc.base

import java.util.function.IntConsumer

/**
  * Represents a query with named parameters.
  *
  * @param queryText is the text of the query after replacing parameter names with '?'.
  * @param originalQueryText is the text of the query before replacing the parameter names with '?'.
  * @param parameterPositions is the map containing the positions of each parameter name.
  */
case class CompiledStatement private (
  queryText: String,
  originalQueryText: String,
  parameterPositions: Map[String, Set[Int]]
)

object CompiledStatement {

  /**
    * Given a query with named parameters beginning with '@',
    * construct the query for use with JDBC, so that names
    * are replaced by '?', and each parameter
    * has a map to its positions in the query.
    *
    * Parameter names must start with a unicode letter or underscore, and then
    * any character after the first one can be a unicode letter, unicode number,
    * or underscore. A parameter that does not follow
    * this scheme must be quoted by backticks. Parameter names
    * are case sensitive.
    *
    * Two '@' in a row are replaced with a single '@'. This follows the same
    * rules as [[String#replace(String, String)]], so that "@@@abc" becomes
    * "@?".
    *
    * Examples of identifiers:
    *
    * {{{"@hello"}}}
    *
    * {{{"@`hello there`"}}}
    *
    * {{{"@_i_am_busy"}}}
    *
    * @param queryText
    */
  def apply(
    queryText: String
  ): CompiledStatement = {
    val parts = findParts(queryText)
    val positions = findParameterPositions(parts)
    val statement = partsToStatement(parts)
    CompiledStatement(statement, queryText, positions)
  }

  def literal(
    queryText: String
  ): CompiledStatement = {
    CompiledStatement(queryText, queryText, Map.empty[String, Set[Int]])
  }

  /**
   * Since the original variable names can't be gotten from
   * the string context (they're replaced by empty strings),
   * use numbers to represent the parameter names, starting
   * from 0.
    *
    * @param sc
   * @return
   */
  def apply(sc: StringContext): CompiledStatement = {
    val builder = new StringBuilder()
    val parts = sc.parts.iterator
    var i = 0

    builder.append(StringContext.treatEscapes(parts.next()))

    while(parts.hasNext) {
      builder.append(s"@`$i`")
      i += 1
      builder.append(StringContext.treatEscapes(parts.next()))
    }

    val queryText = builder.toString

    apply(queryText)
  }

  private val CodePointAt = Character.codePointAt("@", 0)

  private val CodePointQuote = Character.codePointAt("`", 0)

  private val CodePointUnderscore = Character.codePointAt("_", 0)

  def isParameterChar(c: Int): Boolean = {
    c == CodePointUnderscore || Character.isLetterOrDigit(c)
  }

  private case class ParamAccum(paramChars: Vector[Int], inQuote: Boolean = false) {
    def toParameter =
      Parameter(paramChars)

    def append(codePoint: Int): ParamAccum =
      copy(paramChars :+ codePoint)
  }

  private case class FindPartsAccum(
    accum: Vector[QueryPart],maybeParamAccum: Option[ParamAccum],
    literalAccum: Vector[Int]
  ) {
    def append(codePoint: Int): FindPartsAccum = {
      (codePoint, maybeParamAccum) match {
        //Begin parameter
        case (CodePointAt, None) =>
          copy(
            accum = accum :+ QueryText(literalAccum),
            maybeParamAccum = Some(ParamAccum(Vector(), false)),
            literalAccum = Vector()
          )

        //Parameter was begun, and it is quoted
        case (CodePointQuote, Some(ParamAccum(Vector(), false))) =>
          copy(maybeParamAccum = Some(ParamAccum(Vector(), true)))

        //unquoted Parameter was begun, and is continuing
        case (codePoint, Some(paramAccum@ParamAccum(paramChars, false))) if isParameterChar(codePoint) =>
          copy(maybeParamAccum = Some(paramAccum.append(codePoint)))

        //quoted Parameter was begun, and is continuing
        case (codePoint, Some(paramAccum@ParamAccum(_, true))) if codePoint != CodePointQuote =>
          copy(maybeParamAccum = Some(paramAccum.append(codePoint)))

        //We thought we were starting a parameter, but really it was a literal '@'
        case (CodePointAt, Some(ParamAccum(Vector(), false))) =>
          copy(
            maybeParamAccum = None,
            literalAccum = Vector(CodePointAt)
          )

        //We were in a parameter, but a new one is beginning
        case (CodePointAt, Some(paramAccum@ParamAccum(_, false))) =>
          copy(
            accum = accum :+ paramAccum.toParameter,
            maybeParamAccum = Some(ParamAccum(Vector(), false))
          )

        //ending a quoted parameter
        case (CodePointQuote, Some(paramAccum@ParamAccum(_, true))) =>
          copy(
            accum = accum :+ paramAccum.toParameter,
            maybeParamAccum = None
          )

        //ending an unquoted parameter
        case (codePoint, Some(paramAccum)) =>
          copy(
            accum :+ paramAccum.toParameter,
            maybeParamAccum = None,
            literalAccum = Vector(codePoint)
          )

        //continue string literal
        case (otherwise, None) =>
          copy(literalAccum = literalAccum :+ otherwise)
      }
    }

    /**
      * Use to get the final query parts collection when the end of the query is reached.
      *
      * @return
      */
    def finish: Vector[QueryPart] = {
      if (maybeParamAccum.exists(_.inQuote)) throw new IllegalStateException("missing end of quoted parameter")
      if (maybeParamAccum.exists(_.paramChars.isEmpty)) throw new IllegalStateException("missing parameter name at end of query")
      accum :+ maybeParamAccum.map(_.toParameter).getOrElse(QueryText(literalAccum))
    }
  }

  private object FindPartsAccum {
    val empty = FindPartsAccum(accum = Vector(), maybeParamAccum = None, literalAccum = Vector())
  }

  private def findParts(value: String): Vector[QueryPart] = {

    val codePoints = collection.mutable.Buffer.empty[Int]

    value.codePoints.forEach(new IntConsumer {
      override def accept(value: Int): Unit = {
        codePoints.append(value)
      }
    })

    codePoints.foldLeft(FindPartsAccum.empty)(_.append(_)).finish
  }

  private def findParameterPositions(parts: Vector[QueryPart]): Map[String, Set[Int]] = {
    parts.collect{case p: Parameter => p.toString}.zipWithIndex.foldLeft(Map.empty[String, Set[Int]]){
      case (positionMap, (name, index)) =>
        positionMap.get(name) match {
          case None =>
            positionMap + (name -> Set(index))
          case Some(parameterPositions) =>
            positionMap + (name -> (parameterPositions + index))
        }
    }
  }

  private def partsToStatement(parts: Vector[QueryPart]): String = {
    parts.map {
      case _: Parameter => "?"
      case q: QueryText => q.toString
    }.mkString
  }
}
