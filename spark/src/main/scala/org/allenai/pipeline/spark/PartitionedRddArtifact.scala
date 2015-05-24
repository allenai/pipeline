package org.allenai.pipeline.spark

import org.allenai.pipeline._

trait PartitionedRddArtifact[+PA <: Artifact] extends Artifact {
  def makePartitionArtifact: Int => PA
  def getExistingPartitionArtifacts: Iterable[PA]
  def markWriteSuccess(): Unit
}