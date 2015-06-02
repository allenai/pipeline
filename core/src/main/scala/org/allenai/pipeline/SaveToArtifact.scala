package org.allenai.pipeline

case class SaveToArtifact[T, A <: Artifact](
    input: Producer[T],
    writer: Serializer[T, A],
    artifact: A
) extends Producer[A] with BasicPipelineStepInfo {
  def create: A = {
    if (!artifact.exists) {
      writer.write(input.get, artifact)
    }
    artifact
  }

}
