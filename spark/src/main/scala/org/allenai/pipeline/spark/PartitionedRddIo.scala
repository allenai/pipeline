package org.allenai.pipeline.spark

import org.allenai.pipeline._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by rodneykinney on 5/24/15.
 */
class PartitionedRddIo[T: ClassTag : StringSerializable](
  sc: SparkContext) extends ArtifactIo[RDD[T], PartitionedRddArtifact[FlatArtifact]]
with BasicPipelineStepInfo {
  override def write(data: RDD[T], artifact: PartitionedRddArtifact[FlatArtifact]): Unit = {
    val makeArtifact = artifact.makePartitionArtifact
    val format = implicitly[StringSerializable[T]]
    val convertToString: T => String = SerializeFunction(format.toString)
    val stringRdd = data.map(convertToString)
    val savedPartitions = stringRdd.mapPartitionsWithIndex {
      case (index, data) =>
        import org.allenai.pipeline.IoHelpers._
        val io = LineIteratorIo.text[String]
        val artifact = makeArtifact(index)
        io.write(data, artifact)
        Iterator(1)
    }
    // Force execution of Spark job
    savedPartitions.sum()
  }

  override def read(artifact: PartitionedRddArtifact[FlatArtifact]): RDD[T] = {
    val partitionArtifacts = artifact.getExistingPartitionArtifacts.toVector
    val partitions = sc.parallelize(partitionArtifacts, partitionArtifacts.size)
    val stringRdd = partitions.mapPartitions {
      import org.allenai.pipeline.IoHelpers._
      val io = LineIteratorIo.text[String]
      artifacts =>
        for {
          a <- artifacts
          row <- io.read(a)
        } yield row
    }
    val convertToObject = implicitly[StringSerializable[T]].fromString _
    stringRdd.map(convertToObject)
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

