package org.allenai.pipeline.hackathon

import java.net.URI
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

    def replicatedDirProducer(source: URI): Producer[File] = {
      val dir = pipeline.artifactFactory.createArtifact[DirectoryArtifact](source).dir
      ReplicateDirectory(dir, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    }

    def replicatedFileProducer(source: URI): Producer[File] = {
      val file = pipeline.artifactFactory.createArtifact[FileArtifact](source).file
      ReplicateFile(file, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    }

    // 1. Create the Package steps
    script.packages foreach {
      case Package(id, source) =>
        val producer = replicatedDirProducer(source)
        producers(id) = producer
    }

    // 2. Create the RunProcess steps
    script.stepCommands foreach { stepCommand =>
      val args: Seq[ProcessArg] = stepCommand.tokens map {

        case CommandToken.PackagedInput(packageId, path) =>
          InputFileArg(packageId, FileInDirectory(producers(packageId), path))

        case CommandToken.InputDir(source, maybeId) =>
          val producer = replicatedDirProducer(source)
          val id = maybeId.getOrElse(source.toString)
          producers(id) = producer
          cacheArg(id)(InputFileArg(id, producer))

        case CommandToken.InputFile(source, maybeId) =>
          val producer = replicatedFileProducer(source)
          val id = maybeId.getOrElse(source.toString)
          cacheArg(id)(InputFileArg(id, producer))

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
