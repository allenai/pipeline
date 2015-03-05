package org.allenai.pipeline

case class SaveToArtifact[T, A <: Artifact](
  val input: Producer[T],
  val writer: SerializeToArtifact[T, A],
  val artifact: A) extends Producer[A] with BasicPipelineStepInfo {
  def create: A = {
    if (!artifact.exists)
      writer.write(input.get, artifact)
    artifact
  }

}
