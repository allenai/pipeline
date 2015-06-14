package org.allenai.pipeline.spark

import java.io.InputStream

import org.allenai.pipeline.{ Ai2StepInfo, Producer, StreamClosingIterator }
import org.apache.spark.rdd.RDD

import scala.io.Source

/** Convert an RDD of InputStream sources into an RDD of String
  */
case class ReadStreamContents(
  streams: Producer[RDD[() => InputStream]]
)
    extends Producer[RDD[String]] with Ai2StepInfo {
  override def create = {
    streams.get.flatMap {
      f =>
        StreamClosingIterator(f()) {
          is => Source.fromInputStream(is).getLines
        }
    }
  }
}

