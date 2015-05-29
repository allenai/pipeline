package org.allenai.pipeline.spark

import org.allenai.pipeline._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/24/15.
  */
class PartitionedRddIo[T: ClassTag: StringSerializable](
  sc: SparkContext
) extends ArtifactIo[RDD[T], PartitionedRddArtifact]
    with BasicPipelineStepInfo {
  override def write(data: RDD[T], artifact: PartitionedRddArtifact): Unit = {
    val makeArtifact = artifact.makePartitionArtifact
    val convertToString = SerializeFunction(implicitly[StringSerializable[T]].toString)
    val stringRdd = data.map(convertToString)
    val savedPartitions = stringRdd.mapPartitionsWithIndex {
      case (index, partitionData) =>
        import org.allenai.pipeline.IoHelpers._
        val io = LineIteratorIo.text[String]
        val artifact = makeArtifact(index)
        io.write(partitionData, artifact)
        Iterator(1)
    }
    // Force execution of Spark job
    savedPartitions.count()
    artifact.saveWasSuccessful()
  }

  override def read(artifact: PartitionedRddArtifact): RDD[T] = {
    val partitionArtifacts = artifact.getExistingPartitionArtifacts.toVector
    if (partitionArtifacts.nonEmpty) {
      val partitions = sc.parallelize(partitionArtifacts, partitionArtifacts.size)
      val stringRdd = partitions.mapPartitions {
        artifacts =>
          import org.allenai.pipeline.IoHelpers._
          val io = LineIteratorIo.text[String]
          for {
            a <- artifacts
            row <- io.read(a)
          } yield row
      }
      if (stringRdd.take(1).length > 0) {
        val convertToObject = SerializeFunction(implicitly[StringSerializable[T]].fromString)
        stringRdd.map(convertToObject)
      } else {
        sc.emptyRDD[T]
      }
    } else {
      sc.emptyRDD[T]
    }
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

