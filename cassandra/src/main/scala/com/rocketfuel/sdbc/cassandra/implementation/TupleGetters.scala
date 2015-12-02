package com.rocketfuel.sdbc.cassandra.implementation

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import com.datastax.driver.core.{TupleValue, UDTValue}
import com.google.common.reflect.TypeToken
import com.rocketfuel.sdbc.base
import scodec.bits.ByteVector
import scala.collection.convert.wrapAsScala._

trait TupleGetter[+T] extends base.Getter[TupleValue, Int, T]

private[sdbc] object TupleGetter {

  def apply[T](getter: TupleValue => Int => T): TupleGetter[T] = {
    new TupleGetter[T] {
      override def apply(
        tuple: TupleValue,
        ix: Int
      ): Option[T] = {
        if (tuple.isNull(ix)) None
        else Some(getter(tuple)(ix))
      }
    }
  }

  implicit val UnitTupleGetter: TupleGetter[Unit] = TupleGetter[Unit](row => ix => ())

  implicit val BooleanTupleGetter: TupleGetter[Boolean] = TupleGetter[Boolean](row => ix => row.getBool(ix))

  implicit val BoxedBooleanTupleGetter: TupleGetter[java.lang.Boolean] = TupleGetter[java.lang.Boolean](row => ix => row.getBool(ix))

  implicit val ByteVectorTupleGetter: TupleGetter[ByteVector] = TupleGetter[ByteVector](row => ix => ByteVector(row.getBytes(ix)))

  implicit val ByteBufferTupleGetter: TupleGetter[ByteBuffer] = TupleGetter[ByteBuffer](row => ix => row.getBytes(ix))

  implicit val ArrayByteTupleGetter: TupleGetter[Array[Byte]] = TupleGetter[Array[Byte]](row => ix => ByteVector(row.getBytes(ix)).toArray)

  implicit val DateTupleGetter: TupleGetter[Date] = TupleGetter[Date](row => ix => row.getDate(ix))

  implicit val BigDecimalTupleGetter: TupleGetter[BigDecimal] = TupleGetter[BigDecimal](row => ix => row.getDecimal(ix))

  implicit val JavaBigDecimalTupleGetter: TupleGetter[java.math.BigDecimal] = TupleGetter[java.math.BigDecimal](row => ix => row.getDecimal(ix))

  implicit val IntTupleGetter: TupleGetter[Int] = TupleGetter[Int](row => ix => row.getInt(ix))

  implicit val BoxedIntTupleGetter: TupleGetter[java.lang.Integer] = TupleGetter[java.lang.Integer](row => ix => row.getInt(ix))

  implicit val LongTupleGetter: TupleGetter[Long] = TupleGetter[Long](row => ix => row.getLong(ix))

  implicit val BoxedLongTupleGetter: TupleGetter[java.lang.Long] = TupleGetter[java.lang.Long](row => ix => row.getLong(ix))

  implicit val FloatTupleGetter: TupleGetter[Float] = TupleGetter[Float](row => ix => row.getFloat(ix))

  implicit val BoxedFloatTupleGetter: TupleGetter[java.lang.Float] = TupleGetter[java.lang.Float](row => ix => row.getFloat(ix))

  implicit val DoubleTupleGetter: TupleGetter[Double] = TupleGetter[Double](row => ix => row.getDouble(ix))

  implicit val BoxedDoubleTupleGetter: TupleGetter[java.lang.Double] = TupleGetter[java.lang.Double](row => ix => row.getDouble(ix))

  implicit val InetTupleGetter: TupleGetter[InetAddress] = TupleGetter[InetAddress](row => ix => row.getInet(ix))

  implicit val StringTupleGetter: TupleGetter[String] = TupleGetter[String](row => ix => row.getString(ix))

  implicit val UUIDTupleGetter: TupleGetter[UUID] = TupleGetter[UUID](row => ix => row.getUUID(ix))

  implicit val TupleValueTupleGetter: TupleGetter[TupleValue] = TupleGetter[TupleValue](row => ix => row.getTupleValue(ix))

  implicit val UDTValueTupleGetter: TupleGetter[UDTValue] = TupleGetter[UDTValue](row => ix => row.getUDTValue(ix))

  implicit def SeqTupleGetter[T]: TupleGetter[Seq[T]] = TupleGetter[Seq[T]](row => ix => row.getList[T](ix, new TypeToken[T]() {}))

  implicit def JavaListTupleGetter[T]: TupleGetter[java.util.List[T]] = TupleGetter[java.util.List[T]](row => ix => row.getList[T](ix, new TypeToken[T]() {}))

  implicit def SetTupleGetter[T]: TupleGetter[Set[T]] = TupleGetter[Set[T]](row => ix => row.getSet[T](ix, new TypeToken[T]() {}).toSet)

  implicit def JavaSetTupleGetter[T]: TupleGetter[java.util.Set[T]] = TupleGetter[java.util.Set[T]](row => ix => row.getSet[T](ix, new TypeToken[T]() {}))

  implicit def MapTupleGetter[K, V]: TupleGetter[Map[K, V]] = TupleGetter[Map[K, V]](row => ix => row.getMap[K, V](ix, new TypeToken[K] {}, new TypeToken[V] {}).toMap)

  implicit def JavaMapTupleGetter[K, V]: TupleGetter[java.util.Map[K, V]] = TupleGetter[java.util.Map[K, V]](row => ix => row.getMap[K, V](ix, new TypeToken[K] {}, new TypeToken[V] {}))

  implicit val Tuple0TupleGetter: TupleGetter[Unit] = new TupleGetter[Unit] {
    override def apply(
      row: TupleValue,
      columnIndex: Int
    ): Option[Unit] = {
      if (row.isNull(columnIndex)) None
      else Some(())
    }
  }

  implicit def Tuple1TupleGetter[T0](implicit getter0: TupleGetter[T0]): TupleGetter[(Option[T0])] = new TupleGetter[(Option[T0])] {
    override def apply(
      row: TupleValue,
      columnIndex: Int
    ): Option[(Option[T0])] = {
      Option(row.getTupleValue(columnIndex)).map { case tupleValue =>
        getter0(tupleValue, 0)
      }
    }
  }

  implicit def Tuple2TupleGetter[T0, T1](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1]
  ): TupleGetter[(Option[T0], Option[T1])] = {
    TupleGetter[(Option[T0], Option[T1])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1))
    }
  }

  implicit def Tuple3TupleGetter[T0, T1, T2](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2))
    }
  }

  implicit def Tuple4TupleGetter[T0, T1, T2, T3](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3))
    }
  }

  implicit def Tuple5TupleGetter[T0, T1, T2, T3, T4](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4))
    }
  }

  implicit def Tuple6TupleGetter[T0, T1, T2, T3, T4, T5](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5))
    }
  }

  implicit def Tuple7TupleGetter[T0, T1, T2, T3, T4, T5, T6](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6))
    }
  }

  implicit def Tuple8TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7))
    }
  }

  implicit def Tuple9TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8](implicit
    getter0: TupleGetter[T0],
    getter1: TupleGetter[T1],
    getter2: TupleGetter[T2],
    getter3: TupleGetter[T3],
    getter4: TupleGetter[T4],
    getter5: TupleGetter[T5],
    getter6: TupleGetter[T6],
    getter7: TupleGetter[T7],
    getter8: TupleGetter[T8]
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8))
    }
  }

  implicit def Tuple10TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9))
    }
  }

  implicit def Tuple11TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10))
    }
  }

  implicit def Tuple12TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11))
    }
  }

  implicit def Tuple13TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12))
    }
  }

  implicit def Tuple14TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13))
    }
  }

  implicit def Tuple15TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14))
    }
  }

  implicit def Tuple16TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15))
    }
  }

  implicit def Tuple17TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16))
    }
  }

  implicit def Tuple18TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17))
    }
  }

  implicit def Tuple19TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18))
    }
  }

  implicit def Tuple20TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19))
    }
  }

  implicit def Tuple21TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19), getter20(tupleValue, 20))
    }
  }

  implicit def Tuple22TupleGetter[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](implicit
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
  ): TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20], Option[T21])] = {
    TupleGetter[(Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20], Option[T21])]{ row => ix =>
      val tupleValue = row.getTupleValue(ix)
      (getter0(tupleValue, 0), getter1(tupleValue, 1), getter2(tupleValue, 2), getter3(tupleValue, 3), getter4(tupleValue, 4), getter5(tupleValue, 5), getter6(tupleValue, 6), getter7(tupleValue, 7), getter8(tupleValue, 8), getter9(tupleValue, 9), getter10(tupleValue, 10), getter11(tupleValue, 11), getter12(tupleValue, 12), getter13(tupleValue, 13), getter14(tupleValue, 14), getter15(tupleValue, 15), getter16(tupleValue, 16), getter17(tupleValue, 17), getter18(tupleValue, 18), getter19(tupleValue, 19), getter20(tupleValue, 20), getter21(tupleValue, 21))
    }
  }

}
