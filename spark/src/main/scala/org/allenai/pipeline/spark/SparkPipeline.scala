package org.allenai.pipeline.spark

import java.net.URI

import org.allenai.pipeline._
import org.allenai.pipeline.s3.S3Pipeline

import com.amazonaws.auth.BasicAWSCredentials
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/24/15.
  */
trait SparkPipeline extends Pipeline {
  def sparkContext: SparkContext

  def persistRdd[T: ClassTag: StringSerializable](
    original: Producer[RDD[T]],
    name: String = null,
    suffix: String = ""
  ) = {
    val io = new PartitionedRddIo[T](sparkContext)
    val stepName = Option(name).getOrElse(original.stepInfo.className)
    val path = s"data/$stepName.${hashId(original, io)}$suffix"
    val artifact = createOutputArtifact[PartitionedRddArtifact[FlatArtifact]](path)
    persistToArtifact(original, io, artifact)
  }

  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, CreateRddArtifacts.fromFileUrls)
}

trait SparkS3Pipeline extends SparkPipeline with S3Pipeline {
  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, CreateRddArtifacts.fromS3Urls(credentials))
}

trait ConfiguredSparkPipeline extends SparkPipeline with ConfiguredPipeline {
  override def persistRdd[T: ClassTag: StringSerializable](
    original: Producer[RDD[T]],
    name: String = null,
    suffix: String = ""
  ) = {
    val io = new PartitionedRddIo[T](sparkContext)
    persist(original, io, name, suffix)
  }
}

object SparkPipeline {
  def apply(
    sc: SparkContext,
    rootUrl: URI,
    awsCredentials: BasicAWSCredentials = null
  ) =
    awsCredentials match {
      case null =>
        new SparkPipeline {
          override def rootOutputUrl = rootUrl
          override def sparkContext: SparkContext = sc
        }
      case _ =>
        new SparkS3Pipeline {
          override def rootOutputUrl = rootUrl
          override def credentials = awsCredentials

          override def sparkContext: SparkContext = sc
        }
    }

  def configured(
    cfg: Config,
    sc: SparkContext,
    awsCredentials: BasicAWSCredentials = null
  ) =
    awsCredentials match {
      case null =>
        new ConfiguredSparkPipeline {
          override def sparkContext: SparkContext = sc

          override val config: Config = cfg
        }
      case _ =>
        new ConfiguredSparkPipeline with SparkS3Pipeline {
          override def sparkContext: SparkContext = sc

          override val config: Config = cfg

          override def credentials = awsCredentials

        }
    }
}

