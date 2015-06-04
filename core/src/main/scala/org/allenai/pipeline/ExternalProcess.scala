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

  def run(inputs : Seq[Extarg],
          stdinput: () => InputStream = () => new ByteArrayInputStream(Array.emptyByteArray)) =
  {
    val commandTokensToBind = commandTokens.filter{
      case StringToken(_) => false
      case InputFileToken(_) => true
      case OutputFileToken(_) => false   // handled through CommandOutput.outputs
    }

    {
      val inputTokenNames = commandTokens.map(_.name)
      val cRunBinds = commandTokensToBind.length
      require(cRunBinds == inputs.length, s"found ${inputs.length} but need exactly ${cRunBinds} arguments")
      val outputNames = commandTokens.collect { case OutputFileToken(name) => name}.toSet
    }

    val scratchDir = Files.createTempDirectory(null).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    val commandTokens2 : Seq[CommandToken] = commandTokensToBind.toSeq
    val argsBound : Seq[(_,_,String)] =
      commandTokens2.zip(inputs).zipWithIndex.map{
        case ((name, data), i) =>
          val v : String =
            (name,data) match {
              case (StringToken(s),_) => s
              case (InputFileToken(nameTemp), ExtargStream(b)) =>
                // Write ExtargStream arguments to temporary files
                //
                // TODO: nameTemp = f"tmp$i"
                val f = new File(scratchDir, nameTemp)
                StreamIo.write(b.get, new FileArtifact(f))
                f.getCanonicalPath()
              case _ =>
                throw new ExternalProcessArgException(s"Type mismatch on argument $i")
            }
          (name,data,v)
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

    val cmd : Seq[String] = commandTokens.map {
      case InputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case OutputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case t => t.name
    }

    val status = (cmd #< stdinput()) ! logger
    out.close()
    err.close()

    val outputNames = commandTokens.collect { case OutputFileToken(name) => name}

    val outputStreams = for (name <- outputNames) yield {
      (name, StreamIo.read(new FileArtifact(new File(scratchDir, name))))
    }
    val stdout = StreamIo.read(new FileArtifact(captureStdoutFile))
    val stderr = StreamIo.read(new FileArtifact(captureStderrFile))

    CommandOutput(status, stdout, stderr, outputStreams.toMap)
  }

}

object ExternalProcess {

  trait Namable {
    def name: String
  }

  sealed trait CommandToken extends Namable

  case class StringToken(name: String) extends CommandToken

  case class InputFileToken(name: String) extends CommandToken

  case class OutputFileToken(name: String) extends CommandToken

  /** Things which ExternalProcess is prepared to consume, currently
    * only ExtargStream */
  sealed trait Extarg

  case class ExtargStream(a : Producer[() => InputStream]) extends Extarg

  class ExternalProcessArgException(s : String) extends Throwable

  import scala.language.implicitConversions

  implicit def convertToInputData[T, A <: FlatArtifact](p: PersistedProducer[T, A]): Extarg = {
    ExtargStream(p.copy(create =
      () => {
        p.get
        StreamIo.read(p.artifact.asInstanceOf[FlatArtifact])
      }))
  }

  implicit def convertArtifactToInputData[A <: FlatArtifact](artifact: A): Extarg = {
    ExtargStream(StaticResource(artifact))
  }

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
    val commandTokens: Seq[CommandToken],
    _versionHistory: Seq[String],
    inputs: Seq[Extarg]
) extends Producer[CommandOutput] with Ai2SimpleStepInfo {
    override def create = {
      new ExternalProcess(commandTokens: _*).run(
        inputs.map(x =>
          x match {
            case ExtargStream(v) => v.get; x
          }
        ))
    }

  //val parameters = inputsOld1.map { case (name, src) => (name, src) }.toList

  override def versionHistory = _versionHistory

  override def stepInfo = {
    val cmd = commandTokens.map {
      case InputFileToken(name) => s"<$name>"
      case OutputFileToken(name) => s"<$name>"
      case t => t.name
    }
    // TODO: extract method
    super.stepInfo
      .copy(className = "ExternalProcess")
      .addParameters(commandTokens.map(_.toString()).zip(inputs).toList : _*)
      .addParameters("cmd" -> cmd.mkString(" "))
  }
}
object RunExternalProcess {

  import ExternalProcess.CommandOutput

  def apply(
    commandTokens: CommandToken*
  )(
    inputs: Seq[Extarg],
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

