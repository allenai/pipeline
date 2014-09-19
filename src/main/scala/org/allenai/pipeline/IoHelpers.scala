package org.allenai.pipeline

import spray.json._

/** Utility methods for Artifact reading/writing.  */
object IoHelpers extends ReadHelpers with WriteHelpers {

  import scala.language.implicitConversions

  /** A Pipeline step wrapper for in-memory data. */
  object FromMemory {
    def apply[T](data: T): Producer[T] = new Producer[T] {
      def create = data
      def signature=Signature(data.getClass.getSimpleName, "0", "instance" -> data.hashCode)
    }
  }

  def asStringSerializable[T](jsonFormat: JsonFormat[T]) = new StringSerializable[T] {
    override def fromString(s: String) = jsonFormat.read(s.parseJson)

    override def toString(data: T) = jsonFormat.write(data).compactPrint
  }

}
