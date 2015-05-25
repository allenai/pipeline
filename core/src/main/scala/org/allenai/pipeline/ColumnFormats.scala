package org.allenai.pipeline

import scala.language.implicitConversions
import scala.reflect.ClassTag

import java.util.regex.Pattern

/** Support for persisting to a column-delimited file.
  * Persisted object can be a case-class or Tuple.
  * Each field of the object must be a primitive type (Int, Double, String)
  * and will be written as a column in the output file.
  */
trait ColumnFormats {
  def columnFormat[P1, T <: Product](construct: (P1) => T)(
    implicit
    p1Parser: StringSerializable[P1]
  ): StringSerializable[T] =
    new StringSerializable[T] {
      override def fromString(s: String): T = construct(p1Parser.fromString(s))

      override def toString(t: T): String = {
        p1Parser.toString(t.productElement(0).asInstanceOf[P1])
      }
    }

  def columnFormat2[P1, P2, T <: Product](
    construct: (P1, P2) => T,
    sep: Char = '\t'
  )(
    implicit
    p1Parser: StringSerializable[P1],
    p2Parser: StringSerializable[P2]
  ): StringSerializable[T] = new StringSerializable[T] {
    val p = compile(sep)

    override def fromString(s: String): T = {
      p.split(s, -1) match {
        case Array(p1, p2) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2))
        case x => sys.error(s"Wrong number of columns in TSV (${x.length} instead of 2)")
      }
    }

    override def toString(t: T): String = {
      List(
        p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2])
      ).mkString(sep.toString)
    }
  }

  def columnFormat3[P1, P2, P3, T <: Product](
    construct: (P1, P2, P3) => T,
    sep: Char = '\t'
  )(
    implicit
    p1Parser: StringSerializable[P1],
    p2Parser: StringSerializable[P2],
    p3Parser: StringSerializable[P3]
  ): StringSerializable[T] = new StringSerializable[T] {
    val p = compile(sep)

    override def fromString(s: String): T = {
      p.split(s, -1) match {
        case Array(p1, p2, p3) => construct(
          p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3)
        )
        case x => sys.error(s"Wrong number of columns in TSV (${x.length} instead of 3)")
      }
    }

    override def toString(t: T): String = {
      List(
        p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3])
      ).mkString(sep.toString)
    }
  }

  def columnFormat4[P1, P2, P3, P4, T <: Product](
    construct: (P1, P2, P3, P4) => T,
    sep: Char = '\t'
  )(implicit
    p1Parser: StringSerializable[P1],
    p2Parser: StringSerializable[P2],
    p3Parser: StringSerializable[P3],
    p4Parser: StringSerializable[P4]): StringSerializable[T] = new StringSerializable[T] {
    val p = compile(sep)

    override def fromString(s: String): T = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4) => construct(
          p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3),
          p4Parser.fromString(p4)
        )
        case x => sys.error(s"Wrong number of columns in TSV (${x.length} instead of 4)")
      }
    }

    override def toString(t: T): String = {
      List(
        p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4])
      ).mkString(sep.toString)
    }
  }

  def columnFormat5[P1, P2, P3, P4, P5, T <: Product](
    construct: (P1, P2, P3, P4, P5) => T,
    sep: Char = '\t'
  )(implicit
    p1Parser: StringSerializable[P1],
    p2Parser: StringSerializable[P2],
    p3Parser: StringSerializable[P3],
    p4Parser: StringSerializable[P4],
    p5Parser: StringSerializable[P5]): StringSerializable[T] = new StringSerializable[T] {
    val p = compile(sep)

    override def fromString(s: String): T = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4, p5) => construct(
          p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3),
          p4Parser.fromString(p4),
          p5Parser.fromString(p5)
        )
        case x => sys.error(s"Wrong number of columns in TSV (${x.length} instead of 5)")
      }
    }

    // scalastyle:off
    override def toString(t: T): String = {
      List(
        p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4]),
        p5Parser.toString(t.productElement(4).asInstanceOf[P5])
      ).mkString(sep.toString)
    }
    // scalastyle:on
  }

  private def compile(c: Char): Pattern = Pattern.compile(s"\\Q$c\\E")

  implicit object IntToString extends StringSerializable[Int] {
    override def fromString(s: String): Int = s.toInt

    override def toString(param: Int): String = param.toString
  }

  implicit object LongToString extends StringSerializable[Long] {
    override def fromString(s: String): Long = s.toLong

    override def toString(param: Long): String = param.toString
  }

  implicit object DoubleToString extends StringSerializable[Double] {
    override def fromString(s: String): Double = s.toDouble

    override def toString(param: Double): String = param.toString
  }

  implicit object FloatToString extends StringSerializable[Float] {
    override def fromString(s: String): Float = s.toFloat

    override def toString(param: Float): String = param.toString
  }

  implicit object StringToString extends StringSerializable[String] {
    override def fromString(s: String): String = s

    override def toString(param: String): String = param
  }

  implicit object BooleanToString extends StringSerializable[Boolean] {
    override def fromString(s: String): Boolean = s.toBoolean

    override def toString(param: Boolean): String = param.toString
  }

  def columnArrayFormat[T: StringSerializable: ClassTag](
    sep: Char = '\t'
  ): StringSerializable[Array[T]] = new StringSerializable[Array[T]] {
    val p = compile(sep)
    val colParser = implicitly[StringSerializable[T]]

    override def fromString(line: String): Array[T] = p.split(line, -1).map(colParser.fromString)

    override def toString(arr: Array[T]): String = arr.map(colParser.toString).mkString(sep
      .toString)
  }

  // This typedef is used sparingly to keep lines readable.  It's not used everywhere so the code
  // is still grep-able.
  private type SS[T] = StringSerializable[T]

  implicit def tuple2ColumnFormat[T1: SS, T2: SS](
    sep: Char = '\t'
  ): StringSerializable[(T1, T2)] =
    columnFormat2(Tuple2.apply[T1, T2], sep)

  implicit def tuple3ColumnFormat[T1: SS, T2: SS, T3: SS](
    sep: Char = '\t'
  ): StringSerializable[(T1, T2, T3)] =
    columnFormat3(Tuple3.apply[T1, T2, T3], sep)

  implicit def tuple4ColumnFormat[T1: SS, T2: SS, T3: SS, T4: SS](
    sep: Char = '\t'
  ): StringSerializable[(T1, T2, T3, T4)] =
    columnFormat4(Tuple4.apply[T1, T2, T3, T4], sep)

  implicit def tuple5ColumnFormat[T1: SS, T2: SS, T3: SS, T4: SS, T5: SS](
    sep: Char = '\t'
  ): StringSerializable[(T1, T2, T3, T4, T5)] =
    columnFormat5(Tuple5.apply[T1, T2, T3, T4, T5], sep)

}
