package org.allenai.pipeline

import spray.json._

import java.net.URI

/** Utility methods for Artifact reading/writing.  */
object IoHelpers extends ReadHelpers with WriteHelpers {

  import scala.language.implicitConversions

  object FromMemory {
    @deprecated("Use producer.fromMemory instead.", "2014.01.22-1")
    def apply[T](data: T): Producer[T] = Producer.fromMemory(data)
  }

  def asStringSerializable[T](jsonFormat: JsonFormat[T]): StringSerializable[T] =
    new StringSerializable[T] {
      override def fromString(s: String): T = jsonFormat.read(s.parseJson)

      override def toString(data: T): String = jsonFormat.write(data).compactPrint
    }

}
