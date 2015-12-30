package com.rocketfuel.sdbc.base.jdbc

import com.rocketfuel.sdbc.base.box
import java.sql.{Array => JdbcArray, _}

trait SeqParameter {
  self: DBMS =>

  case class SeqParameter[T] private[sdbc] (
    arrayType: ArrayType[Seq[T]],
    toArray: Seq[T] => Array[AnyRef]
  ) extends Parameter[Seq[T]] {

    override val set: Seq[T] => (PreparedStatement, Int) => PreparedStatement = {
      seq => (statement, ix) =>
        val connection = statement.getConnection
        val array = connection.createArrayOf(arrayType.name, toArray(seq))
        statement.setArray(ix, array)
        statement
    }

  }

  object SeqParameter {
    //Base cases for inductive SeqParameter creation.
    implicit def ofParameter[T](implicit parameter: Parameter[T], arrayType: ArrayType[Seq[T]]): SeqParameter[T] = {
      val mapF: T => AnyRef = parameter match {
        case p: DerivedParameter[_, _] =>
          //We know that the first parameter is T, but the JVM doesn't know.
          elem => box(p.asInstanceOf[DerivedParameter[T, _]].toJdbcType(elem))
        case _ =>
          elem => box(elem)
      }

      SeqParameter[T](
        arrayType = arrayType,
        toArray = seq => seq.map(mapF).toArray
      )
    }

    implicit def ofParameterOption[T](implicit parameter: Parameter[T], arrayType: ArrayType[Seq[Option[T]]]): SeqParameter[Option[T]] = {
      val mapF: Option[T] => AnyRef = parameter match {
        case p: DerivedParameter[_, _] =>
          //We know that the first parameter is T, but the JVM doesn't know.
          elem => elem.map(p.asInstanceOf[DerivedParameter[T, _]].toJdbcType _ andThen box).orNull
        case _ =>
          elem => elem.map(box).orNull
      }

      SeqParameter[Option[T]](
        arrayType = arrayType,
        toArray = seq => seq.map(mapF).toArray
      )
    }

    //Inductive cases.
    implicit def ofSeqParameter[T](implicit parameter: SeqParameter[T], arrayType: ArrayType[Seq[Seq[T]]]): SeqParameter[Seq[T]] = {
      SeqParameter[Seq[T]](
        arrayType = arrayType,
        toArray = seq => seq.map(parameter.toArray).toArray
      )
    }

    implicit def ofSeqOptionParameter[T](implicit parameter: SeqParameter[Option[T]], arrayType: ArrayType[Seq[Seq[Option[T]]]]): SeqParameter[Seq[Option[T]]] = {
      SeqParameter[Seq[Option[T]]](
        arrayType = arrayType,
        toArray = seq => seq.map(parameter.toArray).toArray
      )
    }
  }

  //T doesn't need to be an AnyRef, because one of the steps before creating the jdbc array is boxing.
  sealed trait ArrayType[-T] {
    val name: String
  }

  case class ConcreteArrayType[-T](override val name: String) extends ArrayType[T]

  object ArrayType {
    def ofSeq[T](implicit innerArrayType: ArrayType[T]): ArrayType[Seq[T]] = new ArrayType[Seq[T]] {
      override val name: String = innerArrayType.name
    }

    def ofSeqOption[T](implicit innerArrayType: ArrayType[T]): ArrayType[Seq[Option[T]]] = new ArrayType[Seq[Option[T]]] {
      override val name: String = innerArrayType.name
    }
  }

  case class SeqUpdater[T](
    arrayType: ArrayType[Seq[T]],
    toArray: Seq[T] => Array[AnyRef]
  ) extends Updater[Seq[T]] {
    override def update(row: UpdatableRow, columnIndex: Int, x: Seq[T]): Unit = {
      val connection = row.getStatement.getConnection
      val array = connection.createArrayOf(arrayType.name, toArray(x))
      row.updateArray(columnIndex, array)
    }
  }

  object SeqUpdater {
    implicit def ofSeqParameter[T](implicit parameter: SeqParameter[T], arrayType: ArrayType[Seq[T]]): SeqUpdater[T] = {
      SeqUpdater[T](
        arrayType = arrayType,
        toArray = parameter.toArray
      )
    }

    implicit def ofSeqOptionParameter[T](implicit parameter: SeqParameter[Option[T]], arrayType: ArrayType[Seq[Option[T]]]): SeqUpdater[Option[T]] = {
      SeqUpdater[Option[T]](
        arrayType = arrayType,
        toArray = parameter.toArray
      )
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
