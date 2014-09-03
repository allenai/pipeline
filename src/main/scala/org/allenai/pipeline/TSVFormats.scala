package org.allenai.pipeline

import java.util.regex.Pattern

import org.allenai.common.Resource

import scala.collection.mutable
import scala.reflect.ClassTag

/** Support for persisting to a TSV file
  * Persisted object can be a case-class or Tuple
  * But each field of the object must be a primitive type (Int, Double, String)
  * Each field will be written as a column in the TSV
  */
object TsvFormats {

  trait StringStorable[T] {
    def fromString(s: String): T

    def toString(param: T): String
  }

  class TsvCollectionIO[T: StringStorable] extends ArtifactIO[Iterable[T], FlatArtifact] {
    private val parser = implicitly[StringStorable[T]]
    def read(artifact: FlatArtifact): Iterable[T] = Resource.using(io.Source.fromInputStream(artifact.read)) { src =>
      src.getLines.map(parser.fromString).toList
    }

    def write(data: Iterable[T], artifact: FlatArtifact): Unit = {
      artifact.write { w =>
        for (d <- data) {
          w.println(parser.toString(d))
        }
      }
    }
  }

  class TsvIteratorIO[T: StringStorable] extends ArtifactIO[Iterator[T], FlatArtifact] {
    private val parser = implicitly[StringStorable[T]]
    def read(artifact: FlatArtifact): Iterator[T] =
      StreamClosingIterator(artifact.read) { is =>
        io.Source.fromInputStream(is).getLines.map(parser.fromString)
      }

    def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
      artifact.write { w =>
        for (d <- data) {
          w.println(parser.toString(d))
        }
      }
    }
  }

  def tsvFormat[P1, T <: Product](construct: (P1) => T)(implicit p1Parser: StringStorable[P1]) = new StringStorable[T] {
    def fromString(s: String) = construct(p1Parser.fromString(s))

    def toString(t: T) = {
      p1Parser.toString(t.productElement(0).asInstanceOf[P1])
    }
  }

  def tsvFormat2[P1, P2, T <: Product](construct: (P1, P2) => T, sep: String = "\t")(implicit p1Parser: StringStorable[P1], p2Parser: StringStorable[P2]) = new StringStorable[T] {
    val p = Pattern.compile(sep)

    def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2])).mkString(sep)
    }
  }

  def tsvFormat3[P1, P2, P3, T <: Product](construct: (P1, P2, P3) => T, sep: String = "\t")(implicit p1Parser: StringStorable[P1], p2Parser: StringStorable[P2], p3Parser: StringStorable[P3]) = new StringStorable[T] {
    val p = Pattern.compile(sep)

    def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2), p3Parser.fromString(p3))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3])).mkString(sep)
    }
  }

  def tsvFormat4[P1, P2, P3, P4, T <: Product](construct: (P1, P2, P3, P4) => T, sep: String = "\t")(implicit p1Parser: StringStorable[P1], p2Parser: StringStorable[P2], p3Parser: StringStorable[P3], p4Parser: StringStorable[P4]) = new StringStorable[T] {
    val p = Pattern.compile(sep)

    def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2), p3Parser.fromString(p3), p4Parser.fromString(p4))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4])).mkString(sep)
    }
  }

  def tsvFormat5[P1, P2, P3, P4, P5, T <: Product](construct: (P1, P2, P3, P4, P5) => T, sep: String = "\t")(implicit p1Parser: StringStorable[P1], p2Parser: StringStorable[P2], p3Parser: StringStorable[P3], p4Parser: StringStorable[P4], p5Parser: StringStorable[P5]) = new StringStorable[T] {
    val p = Pattern.compile(sep)

    def fromString(s: String) = {
      p.split(s, -1) match {
        case Array(p1, p2, p3, p4, p5) => construct(p1Parser.fromString(p1), p2Parser.fromString(p2), p3Parser.fromString(p3), p4Parser.fromString(p4), p5Parser.fromString(p5))
        case _ => sys.error("Wrong number of columns in TSV")
      }
    }

    def toString(t: T) = {
      List(p1Parser.toString(t.productElement(0).asInstanceOf[P1]),
        p2Parser.toString(t.productElement(1).asInstanceOf[P2]),
        p3Parser.toString(t.productElement(2).asInstanceOf[P3]),
        p4Parser.toString(t.productElement(3).asInstanceOf[P4]),
        p5Parser.toString(t.productElement(4).asInstanceOf[P5])).mkString(sep)
    }
  }

  implicit object IntToString extends StringStorable[Int] {
    def fromString(s: String) = s.toInt

    def toString(param: Int) = param.toString
  }

  implicit object DoubleToString extends StringStorable[Double] {
    def fromString(s: String) = s.toDouble

    def toString(param: Double) = param.toString
  }

  implicit object FloatToString extends StringStorable[Float] {
    def fromString(s: String) = s.toFloat

    def toString(param: Float) = param.toString
  }

  implicit object StringToString extends StringStorable[String] {
    def fromString(s: String) = s

    def toString(param: String) = param
  }

  implicit object BooleanToString extends StringStorable[Boolean] {
    def fromString(s: String) = s.toBoolean

    def toString(param: Boolean) = param.toString
  }

  def tsvArrayFormat[T: StringStorable: ClassTag](sep: String = "\t") = new StringStorable[Array[T]] {
    val p = Pattern.compile(sep)
    val colParser = implicitly[StringStorable[T]]

    def fromString(line: String) = p.split(line, -1).map(colParser.fromString)

    def toString(arr: Array[T]) = arr.map(colParser.toString).mkString(sep)
  }

  import scala.language.implicitConversions

  private type SS[T] = StringStorable[T]

  implicit def tsvTuple2Format[T1: SS, T2: SS](sep: String = "\t") = tsvFormat2(Tuple2.apply[T1, T2] _, sep)

  implicit def tsvTuple3Format[T1: SS, T2: SS, T3: SS](sep: String = "\t") = tsvFormat3(Tuple3.apply[T1, T2, T3] _)

  implicit def tsvTuple4Format[T1: SS, T2: SS, T3: SS, T4: SS](sep: String = "\t") = tsvFormat4(Tuple4.apply[T1, T2, T3, T4] _)

  implicit def tsvTuple5Format[T1: SS, T2: SS, T3: SS, T4: SS, T5: SS](sep: String = "\t") = tsvFormat5(Tuple5.apply[T1, T2, T3, T4, T5] _)

}
