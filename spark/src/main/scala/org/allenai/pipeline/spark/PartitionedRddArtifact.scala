package org.allenai.pipeline.spark

import org.allenai.pipeline._

trait PartitionedRddArtifact extends Artifact {
  def makePartitionArtifact: Int => FlatArtifact
  def getExistingPartitions: Iterable[Int]
  def saveWasSuccessful(): Unit

  override def toString = s"PartitionedRddArtifact[$url]"
}