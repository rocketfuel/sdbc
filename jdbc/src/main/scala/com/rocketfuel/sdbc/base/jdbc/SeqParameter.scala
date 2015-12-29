package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.box
import java.sql.{Array => JdbcArray, _}
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import shapeless._
import shapeless.ops.hlist.ToTraversable

trait SeqParameter {
  self: DBMS =>

  trait Type[T]

  case class Terminal[T](clazz: ClassTag[T]) extends Type[T] {
    override def toString: String = clazz.toString()
  }

  case class Container[
    T,
    InnerTypes <: HList,
    InnerLUB
  ](container: ClassTag[T],
    innerTypes: InnerTypes
  )(implicit toTraversableAux : ToTraversable.Aux[InnerTypes, List, InnerLUB]
  ) extends Type[T] {

    def innerTypesList: List[InnerLUB] = innerTypes.toList

    override def toString: String = {
      val innerString = innerTypes.toList.reverse.mkString(", ")
      s"""$container[$innerString]"""
    }
  }

  object Type {
    def apply[T](implicit depth: Type[T]): Type[T] = depth

    implicit def terminal[T](implicit ctag: ClassTag[T]): Type[T] =
      Terminal(ctag)

    implicit def container1[
      T[_],
      Elem,
      InnerLUB
    ](implicit ctag: ClassTag[T[Elem]],
      innerType: Type[Elem],
      toTraversableAux : ToTraversable.Aux[Type[Elem] :: HNil, List, InnerLUB]
    ): Type[T[Elem]] = {
      Container[T[Elem], Type[Elem] :: HNil, InnerLUB](ctag, innerType :: HNil)
    }
  }

  case class QSeq private[sdbc] (
    arrayType: String
  ) extends Parameter[Seq[AnyRef]] {

    override val set: (Seq[AnyRef]) => (PreparedStatement, Int) => PreparedStatement = {
      seq => (statement, ix) =>
        val connection = statement.getConnection
        val array = connection.createArrayOf(arrayType, value)
        statement.setArray(ix, array)
        statement
    }

  }

  object QSeq {

    implicit def ofTerminal[T](implicit t: Terminal[T], arrayType: PrimaryArrayType[T]) = {

    }

    implicit def terminalSeqOfPrimaryArrayType[
      ElemT
    ](s: Seq[ElemT]
    )(implicit arrayType: PrimaryArrayType[ElemT]
    ): QSeq = {
      QSeq(s.map(box).toArray, arrayType.name)
    }

    implicit def terminalSeqOfSecondaryArrayType[
      T,
      JdbcType
    ](s: Seq[T]
    )(implicit arrayType: SecondaryArrayType[T, JdbcType]
    ): QSeq = {
      val elems = for {
        elem <- s
      } yield box(arrayType.conversion(elem))

      QSeq(elems.toArray, arrayType.name)
    }

    implicit def nonterminalSeqOfPrimaryArrayType[
      T,
      ElemT
    ](s: Seq[T]
    )(implicit innerSeq: T => QSeq,
      arrayType: PrimaryArrayType[ElemT]
    ): QSeq = {
      val elemSeqs = for {
        elem <- s
      } yield innerSeq(elem).value

      QSeq(elemSeqs.toArray, arrayType.name)
    }

    implicit def nonterminalSeqOfSecondaryArrayType[
      T,
      ElemT,
      JdbcType
    ](s: Seq[T]
    )(implicit innerSeq: T => QSeq,
      arrayType: SecondaryArrayType[ElemT, JdbcType]
    ): QSeq = {
      val elemSeqs = for {
        elem <- s
      } yield innerSeq(elem).value

      QSeq(elemSeqs.toArray, arrayType.name)
    }

  }

  implicit def seqParameterValue[T, Seq[T], ElemT](implicit parameter: Parameter[ElemT])

  implicit object QSeqParameter extends Parameter[QSeq] {
    override val set: QSeq => (Statement, Int) => Statement = {
      (value) => (statement, parameterIndex) =>
      implicit val connection = statement.getConnection
      val array = value.asJdbcArray()
      statement.setArray(parameterIndex, array)
      statement
    }
  }

  implicit def seqOptionSecondaryParameter[T](implicit ttag: TypeTag[T]): SecondaryParameter[Seq[Option[T]], QSeq] =
    new SecondaryParameter[Seq[Option[T]], QSeq]

  implicit def toSeqSecondaryParameter[T](implicit ttag: TypeTag[T]): SecondaryParameter[Seq[T], QSeq] =
    new SecondaryParameter[Seq[T], QSeq]

  //T doesn't need to be an AnyRef, because one of the steps before creating the jdbc array is boxing.
  sealed trait ArrayType[T] {
    val name: String
  }

  case class PrimaryArrayType[T](override val name: String) extends Parameter[Seq[T]] with ArrayType[T] {
    override val set: (Seq[T]) => (PreparedStatement, Int) => PreparedStatement = {
      seq => (statement, ix) =>
        val connection = statement.getConnection
        val array = connection.createArrayOf(name, value)
        statement.setArray(ix, array)
        statement
    }
  }

  case class SecondaryArrayType[
    T,
    JdbcType
  ](override val name: String,
    conversion: T => JdbcType
  ) extends ArrayType[T]

  object ArrayType {
    def apply[T](implicit typeName: ArrayType[T]): ArrayType[T] = typeName

    implicit def ofParameter[T](name: String)(implicit parameter: Parameter[T]): ArrayType[T] ={
      PrimaryArrayType[T](name)
    }

    implicit def ofSecondaryParameter[
      T,
      JdbcType
    ](name: String
    )(implicit secondaryParameter: SecondaryParameter[T, JdbcType]
    ): ArrayType[T] = {
      SecondaryArrayType[T, JdbcType](name, secondaryParameter.toJdbcType)
    }
  }

  trait SeqParameter[T] extends Parameter[T]

  def parameterToSeqOptionParameter[T](implicit parameter: Parameter[T], tag: TypeTag[T]): SecondaryParameter[Seq[Option[T]], QSeq] = {
    new SecondaryParameter
  }

  def parameterToSeqParameter[T](implicit parameter: Parameter[T], tag: TypeTag[T]): SecondaryParameter[Seq[T], QSeq] = {
    new SecondaryParameter
  }

  implicit def seqOptionUpdater[T](implicit ttag: TypeTag[T]): Updater[Seq[Option[T]]] = {
    new Updater[Seq[Option[T]]] {
      override def update(row: UpdatableRow, columnIndex: Int, x: Seq[Option[T]]): Unit = {
        val q: QSeq = x
        implicit val connection = row.getStatement.getConnection
        val array = q.asJdbcArray()
        row.updateArray(columnIndex, array)
      }
    }
  }

  implicit def seqUpdater[T](implicit ttag: TypeTag[T]): Updater[Seq[T]] = {
    new Updater[Seq[T]] {
      override def update(row: UpdatableRow, columnIndex: Int, x: Seq[T]): Unit = {
        val q: QSeq = x
        implicit val connection = row.getStatement.getConnection
        val array = q.asJdbcArray()
        row.updateArray(columnIndex, array)
      }
    }
  }

  implicit def seqGetter[T](implicit getter: CompositeGetter[T]): Getter[Seq[T]] =
    (row: Row, index: Index) => {
      for {
        array <- Option(row.getArray(index(row)))
      } yield {
        val mappedArray = array.getResultSet.iterator().map { row =>
          row[T](1)
        }
        mappedArray.toSeq
      }
    }

  implicit def seqOptionGetter[T](implicit getter: CompositeGetter[Option[T]]): Getter[Seq[Option[T]]] =
    (row: Row, index: Index) => {
      for {
        array <- Option(row.getArray(index(row)))
      } yield {
        val mappedArray = array.getResultSet.iterator().map { row =>
          row[Option[T]](1)
        }
        mappedArray.toSeq
      }
    }

}
