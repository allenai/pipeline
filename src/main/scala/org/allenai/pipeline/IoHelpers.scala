package org.allenai.pipeline

import spray.json._

import java.net.URI

/** Utility methods for Artifact reading/writing.  */
object IoHelpers extends ReadHelpers with WriteHelpers {

  import scala.language.implicitConversions

  /** A Pipeline step wrapper for in-memory data. */
  object FromMemory {
    def apply[T](data: T): Producer[T] = new Producer[T] with UnknownCodeInfo {
      override def create: T = data

      override def signature: Signature = Signature(data.getClass.getName,
        data.hashCode.toHexString)

      override def outputLocation: Option[URI] = None
    }
  }

  def asStringSerializable[T](jsonFormat: JsonFormat[T]): StringSerializable[T] =
    new StringSerializable[T] {
      override def fromString(s: String): T = jsonFormat.read(s.parseJson)

      override def toString(data: T): String = jsonFormat.write(data).compactPrint
    }

}
