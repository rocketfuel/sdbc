package com.rocketfuel.sdbc

package object base {

  /**
   * Often a row in a database doesn't correspond to exactly one primitive value.
   * Instead, the row decomposes into parts, which then compose into yet another
   * non-primitive value. The parts of the row are indexed usually by an integer
   * or string, but we don't enforce any particular index type.
   *
   * Getters provide a uniform interface for any value that might be stored
   * in a row, when indexed by something.
   */
  type Getter[-Row, -Index, +T] = (Row, Index) => T

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
