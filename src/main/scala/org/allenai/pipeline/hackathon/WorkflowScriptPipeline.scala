package org.allenai.pipeline.hackathon

import org.allenai.pipeline._
import org.allenai.pipeline.s3._

import com.typesafe.config.ConfigFactory

import java.io.File
import java.net.URI
import scala.collection.mutable

object WorkflowScriptPipeline {

  def buildPipeline(rootOutputUrl: URI, lines: Iterable[String]): Pipeline = {
    val script = new PipescriptCompiler().parseLines(rootOutputUrl)(lines)
    buildPipeline(script)
  }

  def buildPipeline(script: WorkflowScript) = {
    val pipeline = S3Pipeline(script.outputDir)

    val producers = mutable.Map[String, Producer[File]]()

    val cachedOutputArgs = mutable.Map[String, OutputArg]()

    val cachedInputArgs = mutable.Map[String, InputArg]()

    def cacheArg(id: String)(arg: => ProcessArg): ProcessArg = {
      val result = arg
      result match {
        case input: InputArg =>
          if (!cachedInputArgs.contains(id)) {
            cachedInputArgs(id) = input
          }
        case output: OutputArg =>
          require(cachedInputArgs.get(id).isEmpty, s"$id already cached!")
          cachedOutputArgs(id) = output
        case a => throw new RuntimeException(s"You CAN'T cache a $a!")
      }
      result
    }

    def replicatedDirProducer(source: URI): Producer[File] = Option(source.getScheme) match {
      case Some("s3") =>
        // TODO: create a producer that reads a directory from S3
        // val artifact = new S3ZipArtifact(source.toString, S3Config(source.getAuthority))
        // val foo = pipeline.s3Cache.readZip(artifact)
        // IoHelpers.Read.fromArtifact()
        val dir = pipeline.artifactFactory.createArtifact[DirectoryArtifact](source).dir
        ReplicateDirectory(dir, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
      case None =>
        val dir = pipeline.artifactFactory.createArtifact[DirectoryArtifact](source).dir
        ReplicateDirectory(dir, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    }

    def replicatedFileProducer(source: URI): Producer[File] = Option(source.getScheme) match {
      case Some("s3") =>
        // TODO: create a producer that reads a file from S3
        val file = pipeline.artifactFactory.createArtifact[FileArtifact](source).file
        ReplicateFile(file, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
      case None =>
        val file = pipeline.artifactFactory.createArtifact[FileArtifact](source).file
        ReplicateFile(file, None, pipeline.rootOutputUrl, pipeline.artifactFactory)
    }

    def replicatedUrlProducer(source: URI): Producer[File] = {
      ReadFromURL(source)
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

        case CommandToken.InputDir(source) =>
          val producer = replicatedDirProducer(source)
          val id = source.toString
          producers(id) = producer
          cacheArg(id)(InputFileArg(id, producer))

        case CommandToken.InputFile(source) =>
          val producer = replicatedFileProducer(source)
          val id = source.toString
          cacheArg(id)(InputFileArg(id, producer))

        case CommandToken.InputUrl(source) =>
          val producer = replicatedUrlProducer(source)
          val id = source.toString
          cacheArg(id)(InputFileArg(id, producer))

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
          producers(id) =
            pipeline.persist(producer, UploadFile, name = id, suffix = stepCommand.outputFiles(id).suffix)
      }
      runProcess.outputDirs foreach {
        case (id, producer) =>
          producers(id) =
            pipeline.persist(producer, UploadDirectory, name = id)
      }
      runProcess
    }
    pipeline
  }
}
