package org.allenai.pipeline

case class ReadFromArtifact[T, A <: Artifact](
  reader: DeserializeFromArtifact[T, A],
  val artifact: A) extends Producer[T] {
  def create: T = {
    require(artifact.exists, s"$artifact does not exist")
    reader.read(artifact)
  }

  override def stepInfo =
    reader.stepInfo.copy(
      parameters = reader.stepInfo.parameters + ("src" -> artifact.url.toString))
}
