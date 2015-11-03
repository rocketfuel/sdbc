package com.rocketfuel.sdbc.cassandra.datastax.implementation

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}
import scodec.bits.ByteVector

import scala.collection.convert.wrapAsScala._
import com.datastax.driver.core.{Row, UDTValue, TupleValue}
import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag

private[sdbc] trait RowGetters {
  self: ParameterValues =>

  implicit val BooleanRowGetter: RowGetter[Boolean] = RowGetters[Boolean](row => ix => row.getBool(ix))

  implicit val BoxedBooleanRowGetter: RowGetter[java.lang.Boolean] = RowGetters[java.lang.Boolean](row => ix => row.getBool(ix))

  implicit val ByteVectorRowGetter: RowGetter[ByteVector] = RowGetters[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

  implicit val DateRowGetter: RowGetter[Date] = RowGetters[Date](row => ix => row.getDate(ix))

  implicit val BigDecimalRowGetter: RowGetter[BigDecimal] = RowGetters[BigDecimal](row => ix => row.getDecimal(ix))

  implicit val JavaBigDecimalRowGetter: RowGetter[java.math.BigDecimal] = RowGetters[java.math.BigDecimal](row => ix => row.getDecimal(ix))

  implicit val IntRowGetter: RowGetter[Int] = RowGetters[Int](row => ix => row.getInt(ix))

  implicit val BoxedIntRowGetter: RowGetter[java.lang.Integer] = RowGetters[java.lang.Integer](row => ix => row.getInt(ix))

  implicit val LongRowGetter: RowGetter[Long] = RowGetters[Long](row => ix => row.getLong(ix))

  implicit val BoxedLongRowGetter: RowGetter[java.lang.Long] = RowGetters[java.lang.Long](row => ix => row.getLong(ix))

  implicit val FloatRowGetter: RowGetter[Float] = RowGetters[Float](row => ix => row.getFloat(ix))

  implicit val BoxedFloatRowGetter: RowGetter[java.lang.Float] = RowGetters[java.lang.Float](row => ix => row.getFloat(ix))

  implicit val DoubleRowGetter: RowGetter[Double] = RowGetters[Double](row => ix => row.getDouble(ix))

  implicit val BoxedDoubleRowGetter: RowGetter[java.lang.Double] = RowGetters[java.lang.Double](row => ix => row.getDouble(ix))

  implicit val InetRowGetter: RowGetter[InetAddress] = RowGetters[InetAddress](row => ix => row.getInet(ix))

  implicit val StringRowGetter: RowGetter[String] = RowGetters[String](row => ix => row.getString(ix))

  implicit val UUIDRowGetter: RowGetter[UUID] = RowGetters[UUID](row => ix => row.getUUID(ix))

  implicit val TupleValueRowGetter: RowGetter[TupleValue] = RowGetters[TupleValue](row => ix => row.getTupleValue(ix))

  implicit val UDTValueRowGetter: RowGetter[UDTValue] = RowGetters[UDTValue](row => ix => row.getUDTValue(ix))

  implicit def SeqRowGetter[T](implicit tTag: ClassTag[T]): RowGetter[Seq[T]] =
    RowGetters[Seq[T]](row => ix => row.getList[T](ix, tTag.runtimeClass.asInstanceOf[Class[T]]))

  implicit def SetRowGetter[T](implicit tTag: ClassTag[T]): RowGetter[Set[T]] =
    RowGetters[Set[T]](row => ix => row.getSet[T](ix, TypeToken.of[T](tTag.runtimeClass.asInstanceOf[Class[T]])).toSet)

  /**
   * This function is broken when it's used implicitly, because valueTag always becomes ClassTag[Nothing], instead of ClassTag[V].
   * @param keyTag
   * @param valueTag
   * @tparam K
   * @tparam V
   * @return
   */
  def MapRowGetter[K, V](keyTag: ClassTag[K], valueTag: ClassTag[V]): RowGetter[Map[K, V]] =
    RowGetters[Map[K, V]](row => ix => row.getMap[K, V](ix, TypeToken.of[K](keyTag.runtimeClass.asInstanceOf[Class[K]]), TypeToken.of[V](valueTag.runtimeClass.asInstanceOf[Class[V]])).toMap)

  implicit val Tuple0RowGetter: RowGetter[Unit] = new RowGetter[Unit] {
    override def apply(
      row: Row,
      ix: Index
    ): Option[Unit] = {
      val columnIndex = ix(row)
      if (row.isNull(columnIndex)) None
      else Some(())
    }
  }

  implicit def Tuple1RowGetter[T0](implicit getter0: TupleGetter[T0]): RowGetter[(Option[T0])] = new RowGetter[(Option[T0])] {
    override def apply(
      row: Row,
      ix: Index
    ): Option[(Option[T0])] = {
      val columnIndex = ix(row)
      Option(row.getTupleValue(columnIndex)).map { case tupleValue =>
        getter0(tupleValue, 0)
      }
    }
  }

  implicit def Tuple2RowGetter[T0, T1](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1]
  ): RowGetter[(Option[T0], Option[T1])] = {
    RowGetters[(Option[T0], Option[T1])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1))
    }
  }

  implicit def Tuple3RowGetter[T0, T1, T2](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2]
  ): RowGetter[(Option[T0], Option[T1], Option[T2])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2))
    }
  }

  implicit def Tuple4RowGetter[T0, T1, T2, T3](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3))
    }
  }

  implicit def Tuple5RowGetter[T0, T1, T2, T3, T4](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4))
    }
  }

  implicit def Tuple6RowGetter[T0, T1, T2, T3, T4, T5](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5))
    }
  }

  implicit def Tuple7RowGetter[T0, T1, T2, T3, T4, T5, T6](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6))
    }
  }

  implicit def Tuple8RowGetter[T0, T1, T2, T3, T4, T5, T6, T7](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7))
    }
  }

  implicit def Tuple9RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8))
    }
  }

  implicit def Tuple10RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9))
    }
  }

  implicit def Tuple11RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10))
    }
  }

  implicit def Tuple12RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11))
    }
  }

  implicit def Tuple13RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12))
    }
  }

  implicit def Tuple14RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13))
    }
  }

  implicit def Tuple15RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14))
    }
  }

  implicit def Tuple16RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15))
    }
  }

  implicit def Tuple17RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16))
    }
  }

  implicit def Tuple18RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16],
    getter17: TupleGetter[T17]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17))
    }
  }

  implicit def Tuple19RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16],
    getter17: TupleGetter[T17],
    getter18: TupleGetter[T18]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18))
    }
  }

  implicit def Tuple20RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16],
    getter17: TupleGetter[T17],
    getter18: TupleGetter[T18],
    getter19: TupleGetter[T19]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19))
    }
  }

  implicit def Tuple21RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16],
    getter17: TupleGetter[T17],
    getter18: TupleGetter[T18],
    getter19: TupleGetter[T19],
    getter20: TupleGetter[T20]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19), getter20(tupleValue, 20))
    }
  }

  implicit def Tuple22RowGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8],
    getter9: TupleGetter[T9],
    getter10: TupleGetter[T10],
    getter11: TupleGetter[T11],
    getter12: TupleGetter[T12],
    getter13: TupleGetter[T13],
    getter14: TupleGetter[T14],
    getter15: TupleGetter[T15],
    getter16: TupleGetter[T16],
    getter17: TupleGetter[T17],
    getter18: TupleGetter[T18],
    getter19: TupleGetter[T19],
    getter20: TupleGetter[T20],
    getter21: TupleGetter[T21]
  ): RowGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20], Option[T21])] = {
    RowGetters[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20], Option[T21])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19), getter20(tupleValue, 20), getter21(tupleValue, 21))
    }
  }

  implicit val ParameterGetter: RowGetter[ParameterValue] =
    new RowGetter[ParameterValue] {
      override def apply(row: Row, ix: Index): Option[ParameterValue] = {

        Option(row.getObject(ix(row))).flatMap {
          case map: java.util.Map[_, _] =>
            //The drive returns NULL maps as empty maps rather than NULL.
            //This means that the result is ambiguous. We'll use None,
            //since that's what we'd expect when pattern matching.
            if (map.isEmpty) None
            else Some(JavaMapToParameter(map))
          case list: java.util.List[_] =>
            //Same semantics as for Map
            if (list.isEmpty) None
            else Some(JavaListToParameter(list))
          case set: java.util.Set[_] =>
            //Same semantics as for Map
            if (set.isEmpty) None
            else Some(JavaSetToParameter(set))
          case l: java.lang.Long =>
            Some(LongToParameter(l.longValue()))
          case b: ByteBuffer =>
            Some(ArrayByteToParameter(b.array()))
          case b: java.lang.Boolean =>
            Some(BooleanToParameter(b.booleanValue()))
          case d: java.math.BigDecimal =>
            Some(JavaBigDecimalToParameter(d))
          case d: java.lang.Double =>
            Some(DoubleToParameter(d.doubleValue()))
          case f: java.lang.Float =>
            Some(FloatToParameter(f.floatValue()))
          case i: InetAddress =>
            Some(InetAddressToParameter(i))
          case i: java.lang.Integer =>
            Some(IntToParameter(i.intValue()))
          case s: String =>
            Some(StringToParameter(s))
          case d: java.util.Date =>
            Some(DateToParameter(d))
          case u: java.util.UUID =>
            Some(UUIDToParameter(u))
          case b: BigInteger =>
            Some(BigIntegerToParameter(b))
          case u: UDTValue =>
            Some(UDTValueToParameter(u))
          case t: TupleValue =>
            Some(TupleValueToParameter(t))
        }

      }
    }
}

private[sdbc] object RowGetters {
  def apply[T](getter: Row => Int => T): RowGetter[T] = {
    new RowGetter[T] {
      override def apply(row: Row, toIx: Index): Option[T] = {
        val ix = toIx(row)
        if (row.isNull(ix)) None
        else Some(getter(row)(toIx(row)))
      }
    }
  }

  def createTupleInstance(arity: Int): String = {
    val builder = new StringBuilder()

    def enumParameters() = {
      for (i <- 0 until arity) {
        builder.append(s"T$i")
        if (i != arity - 1) builder.append(", ")
      }
    }

    def enumImplicitArgs() = {
      for (i <- 0 until arity) {
        builder.append(s"    getter$i: TupleGetter[T$i]")
        if (i != arity - 1) builder.append(",\n")
      }
    }

    def enumOptionResults() = {
      for (i <- 0 until arity) {
        builder.append(s"Option[T$i]")
        if (i != arity - 1) builder.append(", ")
      }
    }

    builder.append(s"  implicit def Tuple${arity}RowGetter[")

    enumParameters()

    builder.append("](implicit\n")

    enumImplicitArgs()

    builder.append("\n  ): RowGetter[(")

    enumOptionResults()

    builder.append(")] = {\n    RowGetters[(")

    enumOptionResults()

    builder.append(")]{ row => ix =>\n      val tupleValue = row.getTupleValue(ix)\n      (")

    for (i <- 0 until arity) {
      builder.append(s"getter$i(tupleValue, $i)")
      if (i != arity - 1) builder.append(", ")
    }

    builder.append(")\n    }\n  }")

    builder.toString()
  }

  def createTupleInstances(): String = {
    val builder = new StringBuilder()

    for (arity <- 1 to 22) {
      builder.append(createTupleInstance(arity))
      builder.append("\n\n")
    }

    builder.toString()
  }
}
