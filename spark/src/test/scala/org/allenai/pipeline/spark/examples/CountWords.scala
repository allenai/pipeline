package org.allenai.pipeline.spark.examples

import org.allenai.pipeline.{ Ai2StepInfo, Producer }

import org.apache.spark.rdd.RDD

/** Created by rodneykinney on 5/24/15.
  */
case class CountWords(lines: Producer[RDD[String]])
    extends Producer[RDD[(String, Int)]] with Ai2StepInfo {
  override def create = {
    val words = lines.get.flatMap(s => s.split("\\s+"))
    words.map(s => (s, 1))
      .reduceByKey(_ + _)
  }
}
