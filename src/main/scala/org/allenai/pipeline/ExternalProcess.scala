package org.allenai.pipeline

import java.io.{ ByteArrayInputStream, File, FileWriter, InputStream }
import java.nio.file.Files
import java.util.UUID

import org.allenai.common.Resource
import org.allenai.pipeline.ExternalProcess._
import org.apache.commons.io.{ FileUtils, IOUtils }

import scala.collection.JavaConverters._

/** Executes an arbitrary system process
  * @param commandTokens   The set of tokens that comprise the command to be executed.
  *                        Each token is either:
  *                        a String
  *                        a Placeholder representing an input data file
  *                        a Placeholder representing an output data file
  *                        Examples:
  *                        StringToken("cp") InputFileToken("src") OutputFileToken("target")
  *                        StringToken("python") InputFileToken("script") StringToken("-o") OutputFileToken("output")
  *
  *                        Producers based on ExternalProcess should be created with class RunExternalProcess.
  */
class ExternalProcess(val commandTokens: CommandToken*) {

  def run(
    inputs: Map[String, () => InputStream] = Map(),
    stdinput: () => InputStream = () => new ByteArrayInputStream(Array.emptyByteArray)
  ) = {
    {
      val inputNames = inputs.map(_._1).toSet
      val inputTokenNames = commandTokens.collect { case InputFileToken(name) => name }.toSet
      val unusedInputs = inputNames -- inputTokenNames
      require(unusedInputs.size == 0, s"The following inputs are not used: [${unusedInputs.mkString(",")}}]")
      val unboundTokens = inputTokenNames -- inputNames
      require(unboundTokens.size == 0, s"The following input tokens were not found: [${unboundTokens.mkString(",")}}]")
      val outputNames = commandTokens.collect { case OutputFileToken(name) => name }.toSet
      require(inputNames.size == inputs.size, "Names of inputs must be unique")
      require((inputNames ++ outputNames).size == inputs.size + outputNames.size, "Cannot share names between inputs and outputs")
      require(((inputNames ++ outputNames) intersect Set("stderr", "stdout")).isEmpty, "Cannot use 'stderr' or 'stdout' for name")
    }

    val scratchDir = Files.createTempDirectory(null).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    for ((name, data) <- inputs) {
      StreamIo.write(data, new FileArtifact(new File(scratchDir, name)))
    }

    import scala.sys.process._
    val captureStdoutFile = new File(scratchDir, "stdout")
    val captureStderrFile = new File(scratchDir, "stderr")
    val out = new FileWriter(captureStdoutFile)
    val err = new FileWriter(captureStderrFile)

    val logger = ProcessLogger(
      (o: String) => out.append(o),
      (e: String) => err.append(e)
    )

    val cmd = commandTokens.map {
      case InputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case OutputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case t => t.name
    }
    val status = (cmd #< stdinput()) ! logger
    out.close()
    err.close()

    commandTokens.foreach {
      case OutputFileToken(name) =>
        val fOut = new File(scratchDir, name)
        if (!fOut.exists()) {
          val stCmd = cmd.mkString(" ")
          throw new RuntimeException(
            f"Script should have written an output file at:${fOut.getCanonicalPath}\n  command=$stCmd"
          )
        }
      case _ =>
    }

    val outputNames = commandTokens.collect { case OutputFileToken(name) => name }

    val outputStreams = for (name <- outputNames) yield {
      (name, StreamIo.read(new FileArtifact(new File(scratchDir, name))))
    }
    val stdout = StreamIo.read(new FileArtifact(captureStdoutFile))
    val stderr = StreamIo.read(new FileArtifact(captureStderrFile))

    CommandOutput(status, stdout, stderr, outputStreams.toMap)
  }

}

object ExternalProcess {

  sealed trait CommandToken {
    def name: String
  }

  case class StringToken(name: String) extends CommandToken

  case class InputFileToken(name: String) extends CommandToken

  case class OutputFileToken(name: String) extends CommandToken

  import scala.language.implicitConversions

  implicit def convertToInputData[T, A <: FlatArtifact](p: PersistedProducer[T, A]): Producer[() => InputStream] = {
    p.copy(create =
      () => {
        p.get
        StreamIo.read(p.artifact.asInstanceOf[FlatArtifact])
      })
  }

  implicit def convertArtifactToInputData[A <: FlatArtifact](artifact: A): Producer[() => InputStream] = StaticResource(artifact)

  implicit def convertToToken(s: String): StringToken = StringToken(s)

  case class CommandOutput(
    returnCode: Int,
    stdout: () => InputStream,
    stderr: () => InputStream,
    outputs: Map[String, () => InputStream]
  )
}

// Pattern: Name Producer subclasses with a verb.

class RunExternalProcess private (
    commandTokens: Seq[CommandToken],
    _versionHistory: Seq[String],
    inputs: Map[String, Producer[() => InputStream]]
) extends Producer[CommandOutput] with Ai2SimpleStepInfo {
  override def create = {
    new ExternalProcess(commandTokens: _*).run(inputs.mapValues(_.get))
  }

  val parameters = inputs.map { case (name, src) => (name, src) }.toList

  override def versionHistory = _versionHistory

  override def stepInfo = {
    val cmd = commandTokens.map {
      case InputFileToken(name) => s"<$name>"
      case OutputFileToken(name) => s"<$name>"
      case t => t.name
    }
    super.stepInfo
      .copy(className = "ExternalProcess")
      .addParameters(parameters: _*)
      .addParameters("cmd" -> cmd.mkString(" "))
  }
}
object RunExternalProcess {

  import ExternalProcess.CommandOutput

  def apply(
    commandTokens: CommandToken*
  )(
    inputs: Map[String, Producer[() => InputStream]] = Map(),
    versionHistory: Seq[String] = Seq(),
    requireStatusCode: Iterable[Int] = List(0)
  ): CommandOutputComponents = {
    val outputNames = commandTokens.collect { case OutputFileToken(name) => name }
    val processCmd = new RunExternalProcess(commandTokens, versionHistory, inputs)
    val baseName = processCmd.stepInfo.className
    val stdout = new ExtractOutputComponent("stdout", _.stdout, processCmd, requireStatusCode)
    val stderr = new ExtractOutputComponent("stderr", _.stderr, processCmd, requireStatusCode)
    val outputStreams =
      for (name <- outputNames) yield {
        val outputProducer =
          new ExtractOutputComponent(s"outputs.$name", _.outputs(name), processCmd, requireStatusCode)
        (name, outputProducer)
      }
    CommandOutputComponents(stdout, stderr, outputStreams.toMap)
  }

  class ExtractOutputComponent(
      name: String,
      f: CommandOutput => () => InputStream,
      processCmd: Producer[CommandOutput],
      requireStatusCode: Iterable[Int] = List(0)
  ) extends Producer[() => InputStream] {
    override protected def create: () => InputStream = {
      val result = processCmd.get
      result match {
        case CommandOutput(status, _, _, _) if requireStatusCode.toSet.contains(status) =>
          f(result)
        case CommandOutput(status, _, stderr, _) =>
          val stderrString = IOUtils.readLines(stderr()).asScala.take(100)
          sys.error(s"Command ${processCmd.stepInfo.parameters("cmd")} failed with status$status: $stderrString")
      }
      f(processCmd.get)
    }

    override def stepInfo: PipelineStepInfo =
      PipelineStepInfo(className = name)
        .addParameters("cmd" -> processCmd)

    def ifSuccessful[T](f: CommandOutput => T): () => T = { () =>
      val result = processCmd.get
      result match {
        case CommandOutput(0, _, _, _) => f(result)
        case CommandOutput(_, _, stderr, _) =>
          sys.error(s" Failed to run command ${processCmd.stepInfo.parameters("cmd")}: $stderr")
      }

    }
  }
}

object StreamIo extends ArtifactIo[() => InputStream, FlatArtifact] {
  override def read(artifact: FlatArtifact): () => InputStream =
    () => artifact.read

  override def write(data: () => InputStream, artifact: FlatArtifact): Unit = {
    artifact.write { writer =>
      val buffer = new Array[Byte](16384)
      Resource.using(data()) { is =>
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
          writer.write(buffer, 0, n))
      }
    }
  }

  override def stepInfo: PipelineStepInfo = PipelineStepInfo(className = "SerializeDataStream")
}

/** Binary data that is assumed to never change.
  * Appropriate for: data in which the URL uniquely determines the content
  */
object StaticResource {
  def apply[A <: FlatArtifact](artifact: A): Producer[() => InputStream] =
    new Producer[() => InputStream] with Ai2SimpleStepInfo {
      override def create = StreamIo.read(artifact)

      override def stepInfo =
        super.stepInfo.copy(className = "StaticResource")
          .copy(outputLocation = Some(artifact.url))
          .addParameters("src" -> artifact.url)
    }
}

object VersionedResource {
  def apply[A <: FlatArtifact](artifact: A, version: String): Producer[() => InputStream] =
    new Producer[() => InputStream] with Ai2SimpleStepInfo {
      override def create = StreamIo.read(artifact)

      override def stepInfo =
        super.stepInfo.copy(
          className = "VersionedResource",
          classVersion = version
        )
          .copy(outputLocation = Some(artifact.url))
          .addParameters("src" -> artifact.url)
    }
}

/** Binary data that is allowed to change.
  * A hash of the contents will be computed
  * to determine whether to rerun downstream pipeline steps
  * Appropriate for: scripts, local resources used during development
  */
object DynamicResource {
  def apply(artifact: FlatArtifact): Producer[() => InputStream] =
    new Producer[() => InputStream] with Ai2SimpleStepInfo {
      lazy val contentHash = {
        var hash = 0L
        val buffer = new Array[Byte](16384)
        Resource.using(artifact.read) { is =>
          Iterator.continually(is.read(buffer)).takeWhile(_ != -1)
            .foreach(n => buffer.take(n).foreach(b => hash = hash * 31 + b))
        }
        hash.toHexString
      }

      override def create = StreamIo.read(artifact)

      override def stepInfo =
        super.stepInfo.addParameters("contentHash" -> contentHash)
          .copy(className = "DynamicResource")
          .copy(outputLocation = Some(artifact.url))
          .addParameters("src" -> artifact.url)

    }
}

/** Binary data that is assumed to change every time it is accessed
  * Appropriate for: non-deterministic queries
  */
object VolatileResource {
  def apply(artifact: FlatArtifact): Producer[() => InputStream] =
    new Producer[() => InputStream] with Ai2SimpleStepInfo {
      override def create = StreamIo.read(artifact)

      override def stepInfo =
        super.stepInfo.addParameters("guid" -> UUID.randomUUID().toString)
          .copy(className = "VolatileResource")
          .copy(outputLocation = Some(artifact.url))
          .addParameters("src" -> artifact.url)
    }
}

// for RunExternalProcess
case class CommandOutputComponents(
  stdout: Producer[() => InputStream],
  stderr: Producer[() => InputStream],
  outputs: Map[String, Producer[() => InputStream]]
)

