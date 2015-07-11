package org.allenai.pipeline.spark

import java.net.URI

import org.allenai.pipeline._
import org.allenai.pipeline.s3.{ S3Credentials, S3Pipeline }

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
    val path = s"$stepName.${hashId(original, io)}$suffix"
    val artifact = createOutputArtifact[PartitionedRddArtifact](path)
    // Similar to Iterators, RDDs are lazy data structures.
    // Caching them in memory will generally force re-calculation of the results
    // Therefore, when persisting, we disable in-memory caching so that
    // the results will always be read from the persistent storage
    persistToArtifact(original, io, artifact, stepName).withCachingDisabled
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
    persist(original, io, name, suffix).withCachingDisabled
  }
}

object SparkPipeline {
  def apply(
    sc: SparkContext,
    rootUrl: URI,
    awsCredentials: S3Credentials = null
  ) =
    awsCredentials match {
      case null =>
        new SparkPipeline {
          def rootOutputDataUrl = ArtifactFactory.appendPath(rootUrl, "data")
          def rootOutputReportUrl = ArtifactFactory.appendPath(rootUrl, "reports")
          override def sparkContext: SparkContext = sc
        }
      case _ =>
        new SparkS3Pipeline {
          def rootOutputDataUrl = ArtifactFactory.appendPath(rootUrl, "data")
          def rootOutputReportUrl = ArtifactFactory.appendPath(rootUrl, "reports")
          override def credentials = awsCredentials

          override def sparkContext: SparkContext = sc
        }
    }

  def configured(
    cfg: Config,
    sc: SparkContext,
    awsCredentials: S3Credentials = null
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

