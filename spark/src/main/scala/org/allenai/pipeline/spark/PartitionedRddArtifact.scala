package org.allenai.pipeline.spark

import org.allenai.pipeline._

trait PartitionedRddArtifact[+A <: Artifact] extends Artifact {
  def makePartitionArtifact: Int => A
  def getExistingPartitionArtifacts: Iterable[A]
  def saveWasSuccessful(): Unit
}