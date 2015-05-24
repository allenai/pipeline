package org.allenai.pipeline.spark

import org.allenai.pipeline._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by rodneykinney on 5/24/15.
 */
abstract class PartitionedRddIo[T : ClassTag, A <: Artifact : ClassTag](
  sc: SparkContext) extends ArtifactIo[RDD[T], PartitionedRddArtifact[A]]
with BasicPipelineStepInfo {
  override def write(data: RDD[T], artifact: PartitionedRddArtifact[A]): Unit = {
    val makeIo = makePartitionIo
    val makeArtifact = artifact.makePartitionArtifact
    val rdd = data.mapPartitionsWithIndex {
      case (index, data) =>
        val io = makeIo()
        val artifact = makeArtifact(index)
        io.write(data, artifact)
        Iterator(1)
    }
    // Force execution of Spark job
    rdd.sum()
  }

  override def read(artifact: PartitionedRddArtifact[A]): RDD[T] = {
    val partitionArtifacts = artifact.getExistingPartitionArtifacts.toVector
    val partitions = sc.parallelize(partitionArtifacts, partitionArtifacts.size)
    val makeIo = makePartitionIo
    val rdd = partitions.mapPartitions {
      val io = makeIo()
      artifacts =>
        for {
          a <- artifacts
          row <- io.read(a)
        } yield row
    }
    rdd
  }

  def makePartitionIo: () => ArtifactIo[Iterator[T], A]
}
