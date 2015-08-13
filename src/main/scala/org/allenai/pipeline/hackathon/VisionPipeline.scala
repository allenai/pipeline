package org.allenai.pipeline.hackathon

import java.io.File
import java.net.URI

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._
import org.allenai.pipeline.s3.S3Pipeline

/** Created by rodneykinney on 8/13/15.
  */
object VisionPipeline extends App {
  val replicate = true

  val pipeline =
    if (replicate)
      S3Pipeline(new URI("s3://ai2-misc/hackathon-2015/pipeline"))
    else
      Pipeline(new File("pipeline-output"))

  val scriptDir: Producer[File] =
    if (replicate)
      ReplicateDirectory(new File("vision-py/scripts"), None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    else
      ReadFromArtifact(UploadDirectory, new DirectoryArtifact(new File("vision-py/scripts")))

  val pngDir: Producer[File] =
    if (replicate)
      ReplicateDirectory(new File("/Users/rodneykinney/Downloads/RegentsRun/regentsImagesResized"), None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    else
      ReadFromArtifact(UploadDirectory, new DirectoryArtifact(new File("/Users/rodneykinney/Downloads/RegentsRun/regentsImagesResized")))

  val arrows = RunProcess(
    "python",
    InputFileArg("script", FileInDirectory(scriptDir, "ExtractArrows.py")),
    "-i",
    InputDirArg("pngDir", pngDir),
    "-o",
    OutputDirArg("arrowsDir")
  )
  val arrowDir = pipeline.persist(arrows.outputDirs("arrowsDir"), UploadDirectory, null, ".zip")

  val blobs = RunProcess(
    "python",
    InputFileArg("script", FileInDirectory(scriptDir, "ExtractBlobs.py")),
    "-i",
    InputDirArg("pngDir", pngDir),
    "-o",
    OutputDirArg("blobsDir")
  )
  val blobDir = pipeline.persist(blobs.outputDirs("blobsDir"), UploadDirectory, null, ".zip")

  val text = RunProcess(
    "python",
    InputFileArg("script", FileInDirectory(scriptDir, "ExtractText.py")),
    "-i",
    InputDirArg("pngDir", pngDir),
    "-o",
    OutputDirArg("textDir")
  )
  val textDir = pipeline.persist(text.outputDirs("textDir"), UploadDirectory, null, ".zip")

  val relations = RunProcess(
    "python",
    InputFileArg("script", FileInDirectory(scriptDir, "ExtractRelations.py")),
    "-a",
    InputDirArg("arrowDir", arrowDir),
    "-b",
    InputDirArg("blobDir", blobDir),
    "-t",
    InputDirArg("textDir", textDir),
    "-o",
    OutputDirArg("relationsDir")
  )
  pipeline.persist(relations.outputDirs("relationsDir"), UploadDirectory, null, ".zip")

  pipeline.run("ExtractRelations")
}

case class FileInDirectory(dir: Producer[File], fileName: String) extends Producer[File] {
  override def create = new File(dir.get, fileName)
  override def stepInfo =
    PipelineStepInfo(fileName)
      .addParameters("dir" -> dir)
}
