package org.allenai.pipeline.spark

import org.allenai.pipeline.{ Ai2StepInfo, Producer, StringSerializable }

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/24/15.
  */
case class DeserializeObject[T: StringSerializable: ClassTag](
    serializedObjects: Producer[RDD[String]]
) extends Producer[RDD[T]] with Ai2StepInfo {
  override def create = {
    val convertToObject = SerializeFunction(implicitly[StringSerializable[T]].fromString)
    serializedObjects.get.map(convertToObject)
  }
}
