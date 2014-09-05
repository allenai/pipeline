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
  private type SS[T] = StringSerializable[T]

  def columnFormat[P1, T <: Product](construct: (P1) => T)(implicit p1Parser: SS[P1]) =
    new SS[T] {
      override def fromString(s: String) = construct(p1Parser.fromString(s))

      override def toString(t: T) = {
        p1Parser.toString(t.productElement(0).asInstanceOf[P1])
      }
    }

  def columnFormat2[P1, P2, T <: Product](construct: (P1, P2) => T, sep: Char = '\t')(implicit p1Parser: SS[P1],
    p2Parser: SS[P2]) = new SS[T] {
    val p = compile(sep)

    override def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    override def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2])).mkString(sep.toString)
    }
  }

  def columnFormat3[P1, P2, P3, T <: Product](construct: (P1, P2, P3) => T,
    sep: Char = '\t')(implicit p1Parser: SS[P1],
      p2Parser: SS[P2], p3Parser: SS[P3]) = new SS[T] {
    val p = compile(sep)

    override def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3) => construct(p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    override def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3])).mkString(sep.toString)
    }
  }

  def columnFormat4[P1, P2, P3, P4, T <: Product](construct: (P1, P2, P3, P4) => T, sep: Char = '\t')(implicit p1Parser: SS[P1],
    p2Parser: SS[P2],
    p3Parser: SS[P3],
    p4Parser: SS[P4]) = new SS[T] {
    val p = compile(sep)

    override def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4) => construct(p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3),
          p4Parser.fromString(p4))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    override def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4])).mkString(sep.toString)
    }
  }

  def columnFormat5[P1, P2, P3, P4, P5, T <: Product](construct: (P1, P2, P3, P4, P5) => T,
    sep: Char = '\t')(implicit p1Parser: SS[P1],
      p2Parser: SS[P2],
      p3Parser: SS[P3],
      p4Parser: SS[P4],
      p5Parser: SS[P5]) = new SS[T] {
    val p = compile(sep)

    override def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4, p5) => construct(p1Parser.fromString(p1),
          p2Parser.fromString(p2),
          p3Parser.fromString(p3),
          p4Parser.fromString(p4),
          p5Parser.fromString(p5))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    override def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4]),
        p5Parser.toString(t.productElement(4).asInstanceOf[P5])).mkString(sep.toString)
    }
  }
  
  private def compile(c: Char): Pattern = Pattern.compile(s"\\Q$c\\E")

  implicit object IntToString extends SS[Int] {
    override def fromString(s: String) = s.toInt

    override def toString(param: Int) = param.toString
  }

  implicit object DoubleToString extends SS[Double] {
    override def fromString(s: String) = s.toDouble

    override def toString(param: Double) = param.toString
  }

  implicit object FloatToString extends SS[Float] {
    override def fromString(s: String) = s.toFloat

    override def toString(param: Float) = param.toString
  }

  implicit object StringToString extends SS[String] {
    override def fromString(s: String) = s

    override def toString(param: String) = param
  }

  implicit object BooleanToString extends SS[Boolean] {
    override def fromString(s: String) = s.toBoolean

    override def toString(param: Boolean) = param.toString
  }

  def columnArrayFormat[T: SS: ClassTag](sep: Char = '\t') = new SS[Array[T]] {
    val p = compile(sep)
    val colParser = implicitly[SS[T]]

    override def fromString(line: String) = p.split(line, -1).map(colParser.fromString)

    override def toString(arr: Array[T]) = arr.map(colParser.toString).mkString(sep.toString)
  }

  implicit def tuple2ColumnFormat[T1: SS, T2: SS](sep: Char = '\t') =
    columnFormat2(Tuple2.apply[T1, T2] _, sep)

  implicit def tuple3ColumnFormat[T1: SS, T2: SS, T3: SS](sep: Char = '\t') =
    columnFormat3(Tuple3.apply[T1, T2, T3] _, sep)

  implicit def tuple4ColumnFormat[T1: SS, T2: SS, T3: SS, T4: SS](sep: Char = '\t') =
    columnFormat4(Tuple4.apply[T1, T2, T3, T4] _, sep)

  implicit def tuple5ColumnFormat[T1: SS, T2: SS, T3: SS, T4: SS, T5: SS](sep: Char = '\t') =
    columnFormat5(Tuple5.apply[T1, T2, T3, T4, T5] _, sep)

}
