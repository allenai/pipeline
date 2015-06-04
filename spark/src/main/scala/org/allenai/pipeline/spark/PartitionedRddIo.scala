package org.allenai.pipeline.spark

import org.allenai.common.Logging
import org.allenai.pipeline._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/24/15.
  */
class PartitionedRddIo[T: ClassTag: StringSerializable](
  sc: SparkContext,
  cleanOutput: Boolean = true
) extends ArtifactIo[RDD[T], PartitionedRddArtifact]
    with BasicPipelineStepInfo with Logging {
  override def write(data: RDD[T], artifact: PartitionedRddArtifact): Unit = {
    if (cleanOutput) {
      deleteArtifact(artifact)
    }
    try {
      val makeArtifact = SerializeFunction(artifact.makePartitionArtifact)
      val convertToString = SerializeFunction(implicitly[StringSerializable[T]].toString)
      val linesCounter = sc.accumulator(0L, s"WriteLineTo(${artifact.url}")
      val stringRdd = data.map {
        s =>
          linesCounter += 1
          convertToString(s)
      }
      val savedPartitions = stringRdd.mapPartitionsWithIndex {
        case (index, partitionData) if partitionData.nonEmpty =>
          import org.allenai.pipeline.IoHelpers._
          val io = LineIteratorIo.text[String]
          val artifact = makeArtifact(index)
          io.write(partitionData, artifact)
          Iterator(1)
        case _ =>
          Iterator(0)
      }
      // Force execution of Spark job
      val partitionCount = savedPartitions.sum()
      logger.info(s"Saved $partitionCount partitions to ${artifact.url}")
      artifact.saveWasSuccessful()
    } catch {
      case ex: Throwable =>
        if (!artifact.exists && cleanOutput) {
          deleteArtifact(artifact)
        }
        throw ex
    }
  }

  override def read(artifact: PartitionedRddArtifact): RDD[T] = {
    val partitionArtifacts = artifact.getExistingPartitions.toVector
    val makeArtifact = SerializeFunction(artifact.makePartitionArtifact)
    if (partitionArtifacts.nonEmpty) {
      val partitions = sc.parallelize(partitionArtifacts, partitionArtifacts.size)
      val stringRdd = partitions.mapPartitions {
        ids =>
          import org.allenai.pipeline.IoHelpers._
          val io = LineIteratorIo.text[String]
          for {
            id <- ids
            a = makeArtifact(id)
            row <- io.read(a)
          } yield row
      }
      val convertToObject = SerializeFunction(implicitly[StringSerializable[T]].fromString)
      val linesCounter = sc.accumulator(0L, s"ReadLineFrom(${artifact.url}")
      stringRdd.map {
        s =>
          linesCounter += 1
          convertToObject(s)
      }
    } else {
      sc.emptyRDD[T]
    }
  }

  protected[this] def deleteArtifact(artifact: PartitionedRddArtifact) = {
    // Clean up directory
    val hdfs = new JobConf(sc.hadoopConfiguration)
    val hdfsDir = FileSystem.get(artifact.url, hdfs)
    hdfsDir.delete(new Path(artifact.url), true)
  }
}

object SerializeFunction {
  def apply[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.Externalizer(f)
    x => locker.get.apply(x)
  }

  def func2[A, B, C](f: (A, B) => C): (A, B) => C = {
    val locker = com.twitter.chill.Externalizer(f)
    (a, b) => locker.get.apply(a, b)
  }
}

