package com.rocketfuel.sdbc.base

import java.io.{FileNotFoundException, InputStream}
import java.net.URL
import java.nio.file.{Files, Path}
import scala.reflect.ClassTag

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
    * @param originalQueryText
    */
  implicit def apply(
    originalQueryText: String
  ): CompiledStatement = {
    val parts = findParts(originalQueryText)
    val positions = findParameterPositions(parts)
    val queryText = partsToStatement(parts)
    CompiledStatement(queryText, originalQueryText, positions)
  }

  /**
    * Construct a CompiledStatement without altering the queryText.
    */
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

    while (parts.hasNext) {
      builder.append(s"@`$i`")
      i += 1
      builder.append(StringContext.treatEscapes(parts.next()))
    }

    val queryText = builder.toString

    apply(queryText)
  }

  def readSource(
    source: scala.io.Source,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    val asString = source.mkString
    if (hasParameters) {
      apply(asString)
    } else literal(asString)
  }

  def readInputStream(
    stream: InputStream,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    readSource(scala.io.Source.fromInputStream(stream), hasParameters)
  }

  def readUrl(
    u: URL,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    val stream = u.openStream()
    try readInputStream(stream, hasParameters)
    finally stream.close()
  }

  def readPath(
    path: Path,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    val bytes = Files.readAllBytes(path)
    val source = io.Source.fromBytes(bytes)
    readSource(source, hasParameters)
  }

  /**
    * Read a query from a resource where constructing the full path to the
    * resource requires the class.
    *
    * Using a different `nameMangler` allows different naming schemes depending
    * on your requirements. The default is to look for resources in the same
    * package as the given class. See [[NameManglers]] for more examples.
    *
    * Name mangling is generally required, because if you have a class a.B, package
    * a.B containing query files can not also exist.
    */
  def readClassResource(
    clazz: Class[_],
    fileName: String,
    nameMangler: (Class[_], String) => String = NameManglers.default,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    val mangledName = nameMangler(clazz, fileName)
    readResource(mangledName, hasParameters)
  }

  /**
    * Read a query from a resource where constructing the full path to the
    * resource requires the class.
    *
    * Using a different `nameMangler` allows different naming schemes depending
    * on your requirements. The default is to look for resources in the same
    * package as the given class. See [[NameManglers]] for more examples.
    *
    * Name mangling is generally required, because if you have a class a.B, package
    * a.B containing query files can not also exist.
    */
  def readTypeResource[A](
    fileName: String,
    nameMangler: (Class[_], String) => String = NameManglers.default,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default,
    tag: ClassTag[A]
  ): CompiledStatement = {
    readClassResource(tag.runtimeClass, fileName, nameMangler, hasParameters)
  }

  def readResource(
    path: String,
    hasParameters: Boolean = true
  )(implicit codec: scala.io.Codec = scala.io.Codec.default
  ): CompiledStatement = {
    val url = getClass.getClassLoader.getResource(path)
    if (url == null)
      throw new FileNotFoundException(path)
    readUrl(url, hasParameters)
  }

  object NameManglers {
    val default: (Class[_], String) => String = samePackage

    /**
      * Given class `a.B`, find query files in `/a/`.
      */
    def samePackage(clazz: Class[_], fileName: String): String = {
      val classPath = clazz.getCanonicalName.split('.').toVector
      val filePath = classPath.init :+ fileName
      filePath.mkString("/")
    }

    /**
      * Given class `a.B`, find query files in `/a/b/`. Only the first
      * character in the class name is lower cased.
      */
    def classToPackage(clazz: Class[_], fileName: String): String = {
      val classPath = clazz.getCanonicalName.split('.').toVector
      val className = classPath.last
      val classNameAsPackage = className.head.toLower +: className.tail
      val filePath = classPath.init :+ classNameAsPackage :+ fileName
      filePath.mkString("/")
    }

    /**
      * Given class `a.B` and prefix `Vector("prefix")`, find query files in
      * `/prefix/a/B/`.
      */
    def withPrefix(prefix: Vector[String])(clazz: Class[_], fileName: String): String = {
      val classPath = clazz.getCanonicalName.split('.').toVector
      val filePath = prefix ++ classPath :+ fileName
      filePath.mkString("/")
    }

    /**
      * Given class `a.B` and suffix `"suffix"`, find query files in
      * `/a/Bsuffix/`.
      */
    def withSuffix(suffix: String)(clazz: Class[_], fileName: String): String = {
      val classPath = clazz.getCanonicalName.split('.').toVector
      val filePath = classPath.init :+ (classPath.last ++ suffix) :+ fileName
      filePath.mkString("/")
    }
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
    accum: Vector[QueryPart],
    maybeParamAccum: Option[ParamAccum],
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
    val codePoints = value.codePoints().toArray
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
