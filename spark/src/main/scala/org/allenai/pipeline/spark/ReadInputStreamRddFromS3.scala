package org.allenai.pipeline.spark

import org.allenai.pipeline.s3.{S3Config, S3FlatArtifact}
import org.allenai.pipeline.{StringSerializable, Ai2SimpleStepInfo, Producer}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import java.io.InputStream

/**
 * Created by rodneykinney on 5/24/15.
 */
case class ReadObjectRddFromS3[T: StringSerializable : ClassTag](
  s3Paths: Producer[Iterable[String]],
  s3Config: S3Config,
  sparkContext: SparkContext,
  numPartitions: Option[Int] = None
  ) extends Producer[RDD[T]] with Ai2SimpleStepInfo {
  override def create = {
    DeserializeObject(
      ReadStreamContents(
        ReadInputStreamRddFromS3(s3Paths, s3Config, numPartitions, sparkContext))).get
  }

  override def stepInfo = {
    val className = scala.reflect.classTag[T].runtimeClass.getSimpleName
    super.stepInfo
      .addParameters(
        "bucket" -> s3Config.bucket,
        "s3Paths" -> s3Paths,
        "numPartitions" -> numPartitions
      )
      .copy(description = Some(s"Read an RDD of [$className] from lines in the input S3 blobs"))
  }
}

case class ReadInputStreamRddFromS3(
  paths: Producer[Iterable[String]],
  s3Config: S3Config,
  numPartitions: Option[Int] = None,
  sparkContext: SparkContext)
  extends Producer[RDD[() => InputStream]] with Ai2SimpleStepInfo {
  override def create = {
    val cfg = s3Config
    val pathsRdd =
      numPartitions
        .map(i => sparkContext.parallelize(paths.get.toVector, i))
        .getOrElse(sparkContext.parallelize(paths.get.toVector))
    val contentsRdd = pathsRdd.map {
      path =>
        () => new S3FlatArtifact(path, cfg).read
    }
    contentsRdd
  }

  override def stepInfo =
    super.stepInfo
      .addParameters(
        "bucket" -> s3Config.bucket,
        "paths" -> paths,
        "numPartitions" -> numPartitions)

}
