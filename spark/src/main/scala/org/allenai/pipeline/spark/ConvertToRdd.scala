package org.allenai.pipeline.spark

import org.allenai.pipeline.{ Ai2SimpleStepInfo, Producer }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/31/15.
  */
case class ConvertToRdd[T: ClassTag](
    collection: Producer[Iterable[T]],
    sparkContext: SparkContext,
    parallelism: Option[Int] = None
) extends Producer[RDD[T]] with Ai2SimpleStepInfo {
  override def create = parallelism match {
    case Some(p) => sparkContext.parallelize(collection.get.toVector, p)
    case None => sparkContext.parallelize(collection.get.toVector)
  }

  override def stepInfo =
    super.stepInfo.addParameters("collection" -> collection)

}
