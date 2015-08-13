package org.allenai.pipeline.hackathon
import org.allenai.pipeline._

object KeyValueIo extends ArtifactIo[Map[String, String], StructuredArtifact] {
  override def write(data: Map[String, String], artifact: StructuredArtifact): Unit = {
    artifact.write { writer =>
      for ((k, v) <- data.toSeq.sortBy(_._1)) {
        writer.writeEntry(k) { (streamWriter) =>
          streamWriter.println(v)
        }
      }
    }
  }

  override def read(artifact: StructuredArtifact): Map[String, String] = ???

  override def stepInfo: PipelineStepInfo = PipelineStepInfo("KeyValueIo")
}
