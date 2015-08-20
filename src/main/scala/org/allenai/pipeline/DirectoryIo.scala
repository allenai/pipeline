package org.allenai.pipeline

import org.allenai.common.Resource

import scala.io.Source

/** Saves a Map[String, String] as a directory of files,
  * where the key is the file name and the value is the file contents
  */
object DirectoryIo extends ArtifactIo[Map[String, String], StructuredArtifact] {
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

  override def stepInfo: PipelineStepInfo = PipelineStepInfo("SaveDirectory")
}
