package org.allenai.pipeline

case class ReadFromArtifact[T, A <: Artifact](
    reader: Deserializer[T, A],
    artifact: A
) extends Producer[T] {
  def create: T = {
    executionMode = ReadFromDisk
    require(artifact.exists, s"$artifact does not exist")
    reader.read(artifact)
  }

  override def stepInfo =
    reader.stepInfo.copy(
      outputLocation = Some(artifact.url)
    )
      .addParameters("src" -> artifact.url)
}
