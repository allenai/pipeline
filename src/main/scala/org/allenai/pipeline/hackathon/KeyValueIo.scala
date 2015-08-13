package org.allenai.pipeline.hackathon

import org.allenai.common.Resource
import org.allenai.pipeline._

import scala.io.Source

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

  override def read(artifact: StructuredArtifact): Map[String, String] = {
    (for ((name, is) <- artifact.reader.readAll) yield {
      (name, Resource.using(is) { i => Source.fromInputStream(i).mkString })
    }).toMap
  }

  override def stepInfo: PipelineStepInfo = PipelineStepInfo("KeyValueIo")
}
