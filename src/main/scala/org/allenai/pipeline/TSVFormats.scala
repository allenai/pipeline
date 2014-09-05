package org.allenai.pipeline

import org.allenai.common.Resource

import scala.io.Source
import scala.reflect.ClassTag

import java.util.regex.Pattern

/** Support for persisting to a TSV file.
  * Persisted object can be a case-class or Tuple.
  * Each field of the object must be a primitive type (Int, Double, String)
  * and will be written as a column in the TSV.
  */
object TsvFormats {

  trait StringStorable[T] {
    def fromString(s: String): T

    def toString(param: T): String
  }

  class TsvCollectionIo[T: StringStorable] extends ArtifactIo[Iterable[T], FlatArtifact] {
    private val parser = implicitly[StringStorable[T]]

    override def read(artifact: FlatArtifact): Iterable[T] = Resource.using(Source.fromInputStream(artifact.read)) { src =>
      src.getLines.map(parser.fromString).toList
    }

    override def write(data: Iterable[T], artifact: FlatArtifact): Unit = {
      artifact.write { w =>
        for (d <- data) {
          w.println(parser.toString(d))
        }
      }
    }
  }

  class TsvIteratorIo[T: StringStorable] extends ArtifactIo[Iterator[T], FlatArtifact] {
    private val parser = implicitly[StringStorable[T]]

    override def read(artifact: FlatArtifact): Iterator[T] =
      StreamClosingIterator(artifact.read) { is =>
        Source.fromInputStream(is).getLines.map(parser.fromString)
      }

    override def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
      artifact.write { w =>
        for (d <- data) {
          w.println(parser.toString(d))
        }
      }
    }
  }

  def tsvFormat[P1, T <: Product](construct: (P1) => T)(implicit p1Parser: StringStorable[P1]) =
    new StringStorable[T] {
      override def fromString(s: String) = construct(p1Parser.fromString(s))

      override def toString(t: T) = {
        p1Parser.toString(t.productElement(0).asInstanceOf[P1])
      }
    }

  def tsvFormat2[P1, P2, T <: Product](construct: (P1, P2) => T, sep: Char = '\t')(implicit p1Parser: StringStorable[P1],
    p2Parser: StringStorable[P2]) = new StringStorable[T] {
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

  def tsvFormat3[P1, P2, P3, T <: Product](construct: (P1, P2, P3) => T,
    sep: Char = '\t')(implicit p1Parser: StringStorable[P1],
      p2Parser: StringStorable[P2], p3Parser: StringStorable[P3]) = new StringStorable[T] {
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

  def tsvFormat4[P1, P2, P3, P4, T <: Product](construct: (P1, P2, P3, P4) => T, sep: Char = '\t')(implicit p1Parser: StringStorable[P1],
    p2Parser: StringStorable[P2],
    p3Parser: StringStorable[P3],
    p4Parser: StringStorable[P4]) = new StringStorable[T] {
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

  def tsvFormat5[P1, P2, P3, P4, P5, T <: Product](construct: (P1, P2, P3, P4, P5) => T,
    sep: Char = '\t')(implicit p1Parser: StringStorable[P1],
      p2Parser: StringStorable[P2],
      p3Parser: StringStorable[P3],
      p4Parser: StringStorable[P4],
      p5Parser: StringStorable[P5]) = new StringStorable[T] {
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

  implicit object IntToString extends StringStorable[Int] {
    override def fromString(s: String) = s.toInt

    override def toString(param: Int) = param.toString
  }

  implicit object DoubleToString extends StringStorable[Double] {
    override def fromString(s: String) = s.toDouble

    override def toString(param: Double) = param.toString
  }

  implicit object FloatToString extends StringStorable[Float] {
    override def fromString(s: String) = s.toFloat

    override def toString(param: Float) = param.toString
  }

  implicit object StringToString extends StringStorable[String] {
    override def fromString(s: String) = s

    override def toString(param: String) = param
  }

  implicit object BooleanToString extends StringStorable[Boolean] {
    override def fromString(s: String) = s.toBoolean

    override def toString(param: Boolean) = param.toString
  }

  def tsvArrayFormat[T: StringStorable: ClassTag](sep: Char = '\t') = new StringStorable[Array[T]] {
    val p = compile(sep)
    val colParser = implicitly[StringStorable[T]]

    override def fromString(line: String) = p.split(line, -1).map(colParser.fromString)

    override def toString(arr: Array[T]) = arr.map(colParser.toString).mkString(sep.toString)
  }

  import scala.language.implicitConversions

  private type SS[T] = StringStorable[T]

  implicit def tsvTuple2Format[T1: SS, T2: SS](sep: Char = '\t') =
    tsvFormat2(Tuple2.apply[T1, T2] _, sep)

  implicit def tsvTuple3Format[T1: SS, T2: SS, T3: SS](sep: Char = '\t') =
    tsvFormat3(Tuple3.apply[T1, T2, T3] _, sep)

  implicit def tsvTuple4Format[T1: SS, T2: SS, T3: SS, T4: SS](sep: Char = '\t') =
    tsvFormat4(Tuple4.apply[T1, T2, T3, T4] _, sep)

  implicit def tsvTuple5Format[T1: SS, T2: SS, T3: SS, T4: SS, T5: SS](sep: Char = '\t') =
    tsvFormat5(Tuple5.apply[T1, T2, T3, T4, T5] _, sep)

}
