package org.allenai.pipeline

import java.io.InputStream

/** Given a function that converts an InputStream into an Iterator,
  * this closes the InputStream when the Iterator has been fully consumed.
  */
object StreamClosingIterator {
  def apply[T](is: InputStream)(makeIterator: InputStream => Iterator[T]): Iterator[T] = {
    val it = makeIterator(is)
    new Iterator[T] {
      private var stillReading = it.hasNext

      override def next(): T = {
        val result = it.next()
        stillReading = it.hasNext
        if (!stillReading) is.close()
        result
      }

      override def hasNext: Boolean = stillReading
    }
  }
}
