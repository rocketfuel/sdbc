package com.rocketfuel.sdbc

package object base {

  def box(v: Any): AnyRef = {
    v match {
      case a: AnyRef => a
      case b: Boolean => Boolean.box(b)
      case b: Byte => Byte.box(b)
      case c: Char => Char.box(c)
      case s: Short => Short.box(s)
      case i: Int => Int.box(i)
      case l: Long => Long.box(l)
      case f: Float => Float.box(f)
      case d: Double => Double.box(d)
      case null => null
    }
  }

  def unbox(v: AnyRef): Any = {
    import java.lang
    v match {
      case b: lang.Boolean => b.booleanValue()
      case b: lang.Byte => b.byteValue()
      case c: lang.Character => c.charValue()
      case s: lang.Short => s.shortValue()
      case i: lang.Integer => i.intValue()
      case l: lang.Long => l.longValue()
      case f: lang.Float => f.floatValue()
      case d: lang.Double => d.doubleValue()
      case otherwise => otherwise
    }
  }

}
