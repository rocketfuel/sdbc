package com.rocketfuel.sdbc.cassandra.implementation

import com.datastax.driver.core.TupleType
import com.rocketfuel.sdbc.base
import com.rocketfuel.sdbc.cassandra.implementation.TupleParameterValues.boxOption
import shapeless._
import shapeless.poly._
import shapeless.syntax.std.tuple._

private[sdbc] trait TupleParameterValues {

  object toSome extends (Id ~> Option) {
    override def apply[T](f: Id[T]): Option[T] = {
      Some(f)
    }
  }

  implicit def Tuple0ToParameterValue(value: Unit): ParameterValue = {
    ParameterValue(TupleType.of().newValue())
  }

  implicit def TupleOption1ToParameterValue[T0](
    value: (Option[T0])
  )(implicit
    dt0: TupleDataType[T0]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType).newValue(boxOption(value))
    ParameterValue(tupleValue)
  }

  implicit def Tuple1ToParameterValue[T0](
    value: (T0)
  )(implicit
    dt0: TupleDataType[T0]
  ): ParameterValue = {
    ParameterValue(Some(value))
  }

  implicit def TupleOption2ToParameterValue[T0, T1](
    value: (Option[T0], Option[T1])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple2ToParameterValue[T0, T1](
    value: (T0, T1)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption3ToParameterValue[T0, T1, T2](
    value: (Option[T0], Option[T1], Option[T2])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple3ToParameterValue[T0, T1, T2](
    value: (T0, T1, T2)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption4ToParameterValue[T0, T1, T2, T3](
    value: (Option[T0], Option[T1], Option[T2], Option[T3])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple4ToParameterValue[T0, T1, T2, T3](
    value: (T0, T1, T2, T3)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption5ToParameterValue[T0, T1, T2, T3, T4](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple5ToParameterValue[T0, T1, T2, T3, T4](
    value: (T0, T1, T2, T3, T4)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption6ToParameterValue[T0, T1, T2, T3, T4, T5](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple6ToParameterValue[T0, T1, T2, T3, T4, T5](
    value: (T0, T1, T2, T3, T4, T5)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption7ToParameterValue[T0, T1, T2, T3, T4, T5, T6](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple7ToParameterValue[T0, T1, T2, T3, T4, T5, T6](
    value: (T0, T1, T2, T3, T4, T5, T6)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption8ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple8ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7](
    value: (T0, T1, T2, T3, T4, T5, T6, T7)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption9ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple9ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption10ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple10ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption11ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple11ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption12ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple12ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption13ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple13ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption14ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple14ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption15ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple15ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption16ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple16ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption17ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple17ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption18ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType, dt17.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple18ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption19ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType, dt17.dataType, dt18.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple19ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption20ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType, dt17.dataType, dt18.dataType, dt19.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple20ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption21ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19],
    dt20: TupleDataType[T20]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType, dt17.dataType, dt18.dataType, dt19.dataType, dt20.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple21ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19],
    dt20: TupleDataType[T20]
  ): ParameterValue = {
    value.map(toSome)
  }

  implicit def TupleOption22ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](
    value: (Option[T0], Option[T1], Option[T2], Option[T3], Option[T4], Option[T5], Option[T6], Option[T7], Option[T8], Option[T9], Option[T10], Option[T11], Option[T12], Option[T13], Option[T14], Option[T15], Option[T16], Option[T17], Option[T18], Option[T19], Option[T20], Option[T21])
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19],
    dt20: TupleDataType[T20],
    dt21: TupleDataType[T21]
  ): ParameterValue = {
    val tupleValue = TupleType.of(dt0.dataType, dt1.dataType, dt2.dataType, dt3.dataType, dt4.dataType, dt5.dataType, dt6.dataType, dt7.dataType, dt8.dataType, dt9.dataType, dt10.dataType, dt11.dataType, dt12.dataType, dt13.dataType, dt14.dataType, dt15.dataType, dt16.dataType, dt17.dataType, dt18.dataType, dt19.dataType, dt20.dataType, dt21.dataType).newValue(value.toList.map(boxOption): _*)
    ParameterValue(tupleValue)
  }

  implicit def Tuple22ToParameterValue[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](
    value: (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)
  )(implicit
    dt0: TupleDataType[T0],
    dt1: TupleDataType[T1],
    dt2: TupleDataType[T2],
    dt3: TupleDataType[T3],
    dt4: TupleDataType[T4],
    dt5: TupleDataType[T5],
    dt6: TupleDataType[T6],
    dt7: TupleDataType[T7],
    dt8: TupleDataType[T8],
    dt9: TupleDataType[T9],
    dt10: TupleDataType[T10],
    dt11: TupleDataType[T11],
    dt12: TupleDataType[T12],
    dt13: TupleDataType[T13],
    dt14: TupleDataType[T14],
    dt15: TupleDataType[T15],
    dt16: TupleDataType[T16],
    dt17: TupleDataType[T17],
    dt18: TupleDataType[T18],
    dt19: TupleDataType[T19],
    dt20: TupleDataType[T20],
    dt21: TupleDataType[T21]
  ): ParameterValue = {
    value.map(toSome)
  }

}

private[sdbc] object TupleParameterValues {
  def boxOption(v: Option[Any]): AnyRef = {
    v.map(base.box).orNull
  }

  def createInstance(arity: Int): String = {
    val builder = new StringBuilder()

    def enumParameters() = {
      for (i <- 0 until arity) {
        builder.append(s"T$i")
        if (i != arity - 1) builder.append(", ")
      }
    }

    def enumOptionParameters() = {
      for (i <- 0 until arity) {
        builder.append(s"Option[T$i]")
        if (i != arity - 1) builder.append(", ")
      }
    }

    def enumImplicitArgs() = {
      for (i <- 0 until arity) {
        builder.append(s"  dt$i: TupleDataType[T$i]")
        if (i != arity - 1) builder.append(",\n")
      }
    }

    builder.append(s"implicit def TupleOption${arity}ToParameterValue[")

    enumParameters()

    builder.append("](\n  value: (")

    enumOptionParameters()

    builder.append(s")\n)(implicit\n")

    enumImplicitArgs()

    builder.append(s"\n): ParameterValue = {\n  val tupleValue = TupleType.of(")

    for (i <- 0 until arity) {
      builder.append(s"dt$i.dataType")
      if (i != arity - 1) builder.append(", ")
    }

    builder.append(").newValue(value.toList.map(boxOption): _*)\n")

    builder.append("  ParameterValue(tupleValue)\n}")

    builder.append(s"\n\nimplicit def Tuple${arity}ToParameterValue[")

    enumParameters()

    builder.append("](\n  value: (")

    enumParameters()

    builder.append(s")\n)(implicit\n")

    enumImplicitArgs()

    builder.append(s"\n): ParameterValue = {\n  value.map(toSome)\n}")

    builder.toString()
  }

  def createInstances(maxArity: Int): String = {
    val builder = new StringBuilder()

    for (arity <- 0 to maxArity) {
      builder.append(createInstance(arity))
      builder.append("\n\n")
    }

    builder.toString()
  }

}
