package org.allenai.pipeline

import java.io.File
import java.net.URI

import org.allenai.pipeline.PipeScript._

import scala.collection.mutable

/** Interprets a PipeScript script and applies its workflow to a Pipeline
  * @param pipeline
  */
class PipeScriptInterpreter(val pipeline: Pipeline) {

  def buildPipeline(scriptText: String): Pipeline = {
    val script = PipeScriptCompiler.compileScript(scriptText)
    buildPipeline(script)
  }

  def buildPipeline(script: PipeScript) = {
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
    script.runCommands foreach { stepCommand =>
      val args: Seq[ProcessArg] = stepCommand.tokens map {

        case CommandToken.PackagedInput(packageId, path) =>
          val dirProducer = producers(packageId)
          val fileProducer = FileInDirectory(dirProducer, path)
          InputFileArg(fileProducer.stepInfo.className, fileProducer)

        case CommandToken.InputDir(source) =>
          val producer = replicatedDirProducer(source)
          val id = source.toString
          producers(id) = producer
          val name = producer.stepInfo.className
          cacheArg(id)(InputDirArg(name, producer))

        case CommandToken.InputFile(source) =>
          val producer = replicatedFileProducer(source)
          val id = source.toString
          val name = producer.stepInfo.className
          cacheArg(id)(InputFileArg(name, producer))

        case CommandToken.InputUrl(source) =>
          val producer = replicatedUrlProducer(source)
          val id = source.toString
          val name = producer.stepInfo.className
          cacheArg(id)(InputFileArg(name, producer))

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
            pipeline.persist(producer, UploadDirectory, name = id, suffix = ".zip")
      }
      runProcess
    }
    pipeline
  }

  def replicatedDirProducer(source: URI): ReplicateDirectory =
    pipeline.artifactFactory.createArtifact[StructuredArtifact](source) match {
      case d: DirectoryArtifact =>
        def createArtifact(name: String) =
          pipeline.artifactFactory.createArtifact[StructuredArtifact](
            pipeline.rootOutputUrl,
            s"uploads/$name.zip"
          )
        ReplicateDirectory(Left((d.dir, createArtifact _)))
      case uploaded =>
        def getName(a: StructuredArtifact) = {
          a.url.getPath.split('/').last.split('.').head
        }
        ReplicateDirectory(Right((uploaded, getName _)))
    }

  def replicatedFileProducer(source: URI): ReplicateFile =
    pipeline.artifactFactory.createArtifact[FlatArtifact](source) match {
      case d: FileArtifact =>
        def createArtifact(name: String) =
          pipeline.artifactFactory.createArtifact[FlatArtifact](
            pipeline.rootOutputUrl,
            s"uploads/$name"
          )
        ReplicateFile(Left((d.file, createArtifact _)))
      case uploaded =>
        def getName(a: FlatArtifact) = {
          a.url.getPath.split('/').last.split('.').head
        }
        ReplicateFile(Right((uploaded, getName _)))
    }

  def makePortable(script: PipeScript) = {
    import CommandToken._
    val packages =
      for (pkg <- script.packages) yield {
        pkg.copy(source = replicatedDirProducer(pkg.source).artifact.url)
      }
    val steps =
      for (cmd <- script.runCommands) yield {
        val tokens =
          for (token <- cmd.tokens) yield token match {
            case InputDir(url) =>
              InputDir(replicatedDirProducer(url).artifact.url)
            case InputFile(url) =>
              InputFile(replicatedFileProducer(url).artifact.url)
            case other => other
          }
        RunCommand(tokens)
      }
    PipeScript(packages, steps)
  }
}
