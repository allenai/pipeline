package org.allenai.pipeline.spark

import org.allenai.common.Resource
import org.allenai.pipeline.{StreamClosingIterator, Ai2StepInfo, Producer}

import org.apache.spark.rdd.RDD

import scala.io.Source

import java.io.InputStream

/**
 * Created by rodneykinney on 5/24/15.
 */
case class ReadStreamContents(
  streams: Producer[RDD[() => InputStream]])
  extends Producer[RDD[String]] with Ai2StepInfo {
  override def create = {
    streams.get.flatMap {
      f => StreamClosingIterator(f()){
        is => Source.fromInputStream(is).getLines
      }
    }
  }
}

