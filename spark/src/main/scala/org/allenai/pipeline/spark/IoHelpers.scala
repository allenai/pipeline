package org.allenai.pipeline.spark

import org.allenai.common.Resource
import org.allenai.pipeline.{PipelineStepInfo, Producer}
import org.allenai.pipeline.s3.{S3FlatArtifact, S3Config}

import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
 * Created by rodneykinney on 5/24/15.
 */
object IoHelpers {
  object ReadRdd {
    def fromS3(s3Config: S3Config,
      sparkContext: SparkContext,
      paths: Iterable[String],
      numPartitions: Option[Int]): Producer[RDD[String]] = {
      new Producer[RDD[String]] {
        override def create = {
          val pathsRdd =
            numPartitions.map(i => sparkContext.parallelize(paths.toVector, i))
              .getOrElse(sparkContext.parallelize(paths.toVector))
          val contentsRdd = pathsRdd.map {
            path =>
              Resource.using(
                Source.fromInputStream(new S3FlatArtifact(path, s3Config).read))(_.mkString)
          }
          contentsRdd
        }
        override def stepInfo = PipelineStepInfo("ReadStringRdd")
      }
    }
  }

}
