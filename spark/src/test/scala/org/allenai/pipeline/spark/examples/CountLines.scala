package org.allenai.pipeline.spark.examples

import org.allenai.pipeline.{Ai2StepInfo, Producer}

import org.apache.spark.rdd.RDD

/**
 * Created by rodneykinney on 5/24/15.
 */
case class CountLines(
  lines: Producer[RDD[String]],
  countBlanks: Boolean = true)
  extends Producer[Long] with Ai2StepInfo {
  override protected def create: Long =
    if (countBlanks)
      lines.get.count()
    else
      lines.get.filter(_.trim.length > 0).count()
}
