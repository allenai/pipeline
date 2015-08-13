package org.allenai.pipeline.hackathon

import java.io.File

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._

/** Created by rodneykinney on 8/13/15.
  */
object VisionPipeline extends App {
  val pipeline = Pipeline(new File("pipeline-output"))

  val scriptDir = ReadFromArtifact(UploadDirectory, new DirectoryArtifact(new File("vision-py/scripts")))
  val pngDir = ReadFromArtifact(UploadDirectory, new DirectoryArtifact(new File("/Users/rodneykinney/Downloads/RegentsRun/regentsImagesResized")))

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

case class FileInDirectory(dir: Producer[File], fileName: String) extends Producer[File] with Ai2StepInfo {
  override def create = new File(dir.get, fileName)
}
