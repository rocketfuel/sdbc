package com.rocketfuel.sdbc.base.jdbc.statement

import com.rocketfuel.sdbc.base.box
import com.rocketfuel.sdbc.base.jdbc._
import java.sql.{Array => _, _}
import scala.xml._

trait SeqParameter {
  self: DBMS =>

  case class SeqParameter[T] private[sdbc](
    arrayType: ArrayTypeName[Seq[T]],
    toArray: Seq[T] => Array[AnyRef]
  ) extends Parameter[Seq[T]] {

    override def apply(v1: Seq[T]): (PreparedStatement, Int) => PreparedStatement = {
      (statement, ix) =>
        val connection = statement.getConnection
        val array = connection.createArrayOf(arrayType.name, toArray(v1))
        statement.setArray(ix + 1, array)
        statement
    }

  }

  object SeqParameter {
    //Base cases for inductive SeqParameter creation.
    implicit def ofParameter[T](implicit
      parameter: Parameter[T],
      arrayType: ArrayTypeName[Seq[T]]
    ): SeqParameter[T] = {
      val mapF: T => AnyRef = parameter match {
        case p: DerivedParameter[_, _] =>
          elem => box(p.convert(elem))
        case _ =>
          elem => box(elem)
      }

      SeqParameter[T](
        arrayType = arrayType,
        toArray = seq => seq.map(mapF).toArray
      )
    }

    implicit def ofParameterOption[T](implicit
      parameter: Parameter[T],
      arrayType: ArrayTypeName[Seq[Option[T]]]
    ): SeqParameter[Option[T]] = {
      val mapF: Option[T] => AnyRef = parameter match {
        case p: DerivedParameter[_, _] =>
          elem => elem.map(p.convert _ andThen box).orNull
        case _ =>
          elem => elem.map(box).orNull
      }

      SeqParameter[Option[T]](
        arrayType = arrayType,
        toArray = seq => seq.map(mapF).toArray
      )
    }

    implicit def ofParameterSome[T](implicit
      parameter: Parameter[T],
      arrayType: ArrayTypeName[Seq[Some[T]]]
    ): SeqParameter[Some[T]] = {
      val mapF: Some[T] => AnyRef = parameter match {
        case p: DerivedParameter[_, _] =>
          elem => elem.map(p.convert _ andThen box).orNull
        case _ =>
          elem => elem.map(box).orNull
      }

      SeqParameter[Some[T]](
        arrayType = arrayType,
        toArray = seq => seq.map(mapF).toArray
      )
    }

    //Inductive cases.
    implicit def ofSeqParameter[T](implicit
      parameter: SeqParameter[T],
      arrayType: ArrayTypeName[Seq[Seq[T]]]
    ): SeqParameter[Seq[T]] = {
      SeqParameter[Seq[T]](
        arrayType = arrayType,
        toArray = seq => seq.map(parameter.toArray).toArray
      )
    }

    implicit def ofSeqOptionParameter[T](implicit
      parameter: SeqParameter[Option[T]],
      arrayType: ArrayTypeName[Seq[Seq[Option[T]]]]
    ): SeqParameter[Seq[Option[T]]] = {
      SeqParameter[Seq[Option[T]]](
        arrayType = arrayType,
        toArray = seq => seq.map(parameter.toArray).toArray
      )
    }
  }

  implicit def ofSeqSomeParameter[T](implicit
    parameter: SeqParameter[Option[T]],
    arrayType: ArrayTypeName[Seq[Seq[Some[T]]]]
  ): SeqParameter[Seq[Some[T]]] = {
    SeqParameter[Seq[Some[T]]](
      arrayType = arrayType,
      toArray = seq => seq.map(parameter.toArray).toArray
    )
  }

  implicit def seqParameterToParameter[T](implicit seqParameter: SeqParameter[T]): Parameter[Seq[T]] = {
    seqParameter
  }

  /**
    * Defines the name of the type to use when creating the JDBC array for type T.
    *
    * @tparam T is the type associated with the given name. It doesn't need to be an AnyRef,
    *           because one of the steps before creating the jdbc array is boxing.
    */
  case class ArrayTypeName[-T](name: String)

  object ArrayTypeName {

    implicit def ofSeq[T](implicit innerArrayType: ArrayTypeName[T]): ArrayTypeName[Seq[T]] =
      innerArrayType.asInstanceOf[ArrayTypeName[Seq[T]]]

    implicit def ofSeqOption[T](implicit innerArrayType: ArrayTypeName[T]): ArrayTypeName[Seq[Option[T]]] =
      innerArrayType.asInstanceOf[ArrayTypeName[Seq[Option[T]]]]

    implicit def ofSeqSome[T](implicit innerArrayType: ArrayTypeName[T]): ArrayTypeName[Seq[Some[T]]] =
      innerArrayType.asInstanceOf[ArrayTypeName[Seq[Some[T]]]]

  }

}

trait SeqUpdater {
  self: DBMS with SelectForUpdate with Updater with SeqParameter =>

  case class SeqUpdater[T](
    arrayType: ArrayTypeName[Seq[T]],
    toArray: Seq[T] => Array[AnyRef]
  ) extends Updater[Seq[T]] {
    override def apply(row: UpdatableRow, columnIndex: Int, x: Seq[T]): Unit = {
      val connection = row.getStatement.getConnection
      val array = connection.createArrayOf(arrayType.name, toArray(x))
      row.updateArray(columnIndex, array)
    }
  }

  implicit def seqUpdaterOfSeqParameter[T](implicit
    parameter: SeqParameter[T],
    arrayType: ArrayTypeName[Seq[T]]
  ): Updater[Seq[T]] = {
    SeqUpdater[T](
      arrayType = arrayType,
      toArray = parameter.toArray
    )
  }

  implicit def seqUpdaterOfSeqOptionParameter[T](implicit
    parameter: SeqParameter[Option[T]],
    arrayType: ArrayTypeName[Seq[Option[T]]]
  ): Updater[Seq[Option[T]]] = {
    SeqUpdater[Option[T]](
      arrayType = arrayType,
      toArray = parameter.toArray
    )
  }

  implicit def seqUpdaterOfSeqSomeParameter[T](implicit
    parameter: SeqParameter[Some[T]],
    arrayType: ArrayTypeName[Seq[Some[T]]]
  ): Updater[Seq[Some[T]]] = {
    SeqUpdater[Some[T]](
      arrayType = arrayType,
      toArray = parameter.toArray
    )
  }

}
