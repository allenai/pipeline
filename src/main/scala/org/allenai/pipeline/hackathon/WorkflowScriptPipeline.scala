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

    val cachedOutputArgs = mutable.Map[String, OutputArg]()

    val cachedInputArgs = mutable.Map[String, InputArg]()

    def cacheArg(id: String)(arg: => ProcessArg): ProcessArg = {
      val result = arg
      result match {
        case input: InputArg => cachedInputArgs(id) = input
        case output: OutputArg => cachedOutputArgs(id) = output
        case a => throw new RuntimeException(s"You CAN'T cache a $a!")
      }
      result
    }

    // 1. Create the Package steps
    script.packages foreach {
      case Package(id, source) =>
        val producer = ReadFromArtifact(
          UploadDirectory,
          pipeline.artifactFactory.createArtifact[DirectoryArtifact](source)
        )
        producers(id) = producer
    }

    // 2. Create the RunProcess steps
    script.stepCommands foreach { stepCommand =>
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

        case CommandToken.PackagedInput(packageId, path) =>
          InputFileArg(packageId, FileInDirectory(producers(packageId), path))

        case CommandToken.Input(source, None) =>
          val producer = ReadFromArtifact(
            UploadFile,
            pipeline.artifactFactory.createArtifact[FlatArtifact](source)
          )
          InputFileArg(source.toString, producer)

        case CommandToken.ReferenceInput(id) => cachedInputArgs(id)

        case CommandToken.ReferenceOutput(id) => cachedOutputArgs(id) match {
          case f: OutputFileArg => InputFileArg(id, producers(id))
          case d: OutputDirArg => InputDirArg(id, producers(id))
        }

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
