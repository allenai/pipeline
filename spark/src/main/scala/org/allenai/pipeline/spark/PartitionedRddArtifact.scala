package org.allenai.pipeline.spark

import org.allenai.pipeline._

trait PartitionedRddArtifact extends Artifact {
  def makePartitionArtifact: Int => FlatArtifact
  def getExistingPartitionArtifacts: Iterable[FlatArtifact]
  def saveWasSuccessful(): Unit
}