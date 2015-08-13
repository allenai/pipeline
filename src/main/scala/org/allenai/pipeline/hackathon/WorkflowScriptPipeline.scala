package org.allenai.pipeline.hackathon

import org.allenai.pipeline._
import org.allenai.pipeline.s3.S3Pipeline

import com.typesafe.config.ConfigFactory

import java.io.File
import scala.collection.mutable

class WorkflowScriptPipeline(script: WorkflowScript) {

  def buildPipeline: Pipeline = {
    val config = ConfigFactory.parseString(s"""
output.dir = "${script.outputDir}"
""")
    val pipeline = S3Pipeline.configured(config)

    val producers = mutable.Map[String, Producer[File]]()

    val referenceArgs = mutable.Map[String, ProcessArg]()

    def cacheArg(id: String)(arg: => ProcessArg): ProcessArg = {
      val result = arg
      referenceArgs(id) = arg
      result
    }

    script.stepCommands map { stepCommand =>
      val args: Seq[ProcessArg] = stepCommand.tokens map {
        case CommandToken.Input(source, Some(id)) => cacheArg(id) {
          producers.get(id) match {
            case Some(producer) => InputFileArg(id, producer)
            case None =>
              val producer = ReadFromArtifact(
                UploadFile,
                pipeline.artifactFactory.createArtifact[FlatArtifact](source)
              )
              producers(id) = producer
              InputFileArg(id, producer)
          }
        }

        case CommandToken.Input(source, None) =>
          val producer = ReadFromArtifact(
            UploadFile,
            pipeline.artifactFactory.createArtifact[FlatArtifact](source)
          )
          InputFileArg(source.toString, producer)

        case CommandToken.ReferenceInput(id) => referenceArgs(id)

        case CommandToken.OutputFile(id, _) => cacheArg(id)(OutputFileArg(id))
        case CommandToken.OutputDir(id) => cacheArg(id)(OutputDirArg(id))
        case CommandToken.StringToken(string) => StringArg(string)
      }
      val runProcess = RunProcess(args: _*)
      runProcess.outputFiles foreach {
        case (id, producer) =>
          producers(id) = producer
          pipeline.persist(producer, UploadFile, name = id, suffix = stepCommand.outputFiles(id).suffix)
      }
      runProcess.outputDirs foreach {
        case (id, producer) =>
          producers(id) = producer
          pipeline.persist(producer, UploadDirectory, name = id)
      }
      runProcess
    }
    pipeline
  }
}

object WorkflowScriptPipelineTester extends App {
  import java.net.URI
  import CommandToken._
  val script = WorkflowScript(
    packages = Seq(
      Package(id = "scripts", source = new URI("./vision-py/scripts"))
    ),
    stepCommands = Seq(
      StepCommand(
        Seq(
          StringToken("python"),
          Input(source = new URI("./vision-py/scripts/ExtractArrows.py")),
          StringToken("-i"),
          Input(source = new URI("./vision-py/png"), id = Some("pngDir")),
          StringToken("-o"),
          OutputDir("arrowDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          Input(source = new URI("./vision-py/scripts/ExtractBlobs.py")),
          StringToken("-i"),
          ReferenceInput("pngDir"),
          StringToken("-o"),
          OutputDir("blobsDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          Input(source = new URI("./vision-py/scripts/ExtractText.py")),
          StringToken("-i"),
          ReferenceInput("pngDir"),
          StringToken("-o"),
          OutputDir("textDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          Input(source = new URI("./vision-py/scripts/ExtractRelations.py")),
          StringToken("--arrows"),
          ReferenceInput("arrowDir"),
          StringToken("--blobs"),
          ReferenceInput("blobsDir"),
          StringToken("--text"),
          ReferenceInput("textDir"),
          StringToken("-o"),
          OutputDir("relationsDir")
        )
      )
    ),
    outputDir = new URI("s3://ai2-s2-dev/pipeline-hackathon/")
  )

  val pipeline = new WorkflowScriptPipeline(script).buildPipeline
}
