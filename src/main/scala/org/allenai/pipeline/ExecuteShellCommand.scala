package org.allenai.pipeline

import java.io.File
import java.nio.file.Files

import ExecuteShellCommand._
import org.apache.commons.io.FileUtils

/** Executes an arbitrary shell command
  * @param inputs A set of named input data resources.
  *               They must exist somewhere, but do not need ot exist on the local filesystem.
  *               They will be copied into a scratch directory for use by the command
  * @param outputs A set of named output data resources.
  *                These are assumed not to exist before the command is run,
  *                and it is assumed that the shell command will create the data for them when it runs
  *                After the command completes, the data will be copied to the named output location,
  *                which need not be on the local filesystem
  * Created by rodneykinney on 5/14/15.
  */
abstract class ExecuteShellCommand(
    val inputs: Iterable[InputData] = List(),
    val outputs: Iterable[OutputData] = List()
) extends Producer[CommandOutput] with Ai2SimpleStepInfo {
  private val inputNames = inputs.map(_.name).toSet
  private val outputNames = outputs.map(_.name).toSet
  require(inputNames.size == inputs.size, "Names of inputs must be unique")
  require(outputNames.size == outputs.size, "Names of outputs must be unique")
  require((inputNames ++ outputNames).size == inputs.size + outputs.size, "Cannot share names between inputs and outputs")
  require(((inputNames ++ outputNames) intersect Set("stderr", "stdout")).isEmpty, "Cannot use 'stderr' or 'stdout' for name")

  /** The set of tokens that comprise the command to be executed.
    * Each token is either:
    *   a String
    *   a Placeholder representing an input data file
    *   a Placeholder representing an output data file
    * Examples:
    *   StringToken("cp") InputFileToken("src") OutputFileToken("target")
    *   StringToken("python") InputFileToken("script") StringToken("-o") OutputFileToken("output")
    * @return
    */
  def commandTokens: Seq[CommandToken]

  def create = {
    val scratchDir = Files.createTempDirectory(s"${stepInfo.className}.${stepInfo.signature.id}").toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    for (InputData(name, data, _) <- inputs) {
      data.copyTo(new FileArtifact(new File(scratchDir, name)))
    }

    import sys.process._
    val out = new StringBuilder
    val err = new StringBuilder

    val logger = ProcessLogger(
      (o: String) => out.append(o),
      (e: String) => err.append(e)
    )

    val cmd = commandTokens.map {
      case InputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case OutputFileToken(name) => new File(scratchDir, name).getCanonicalPath
      case t => t.name
    }
    val status = cmd ! logger

    for (OutputData(name, data) <- outputs) {
      new FileArtifact(new File(scratchDir, name)).copyTo(data)
    }

    CommandOutput(status, out.toString(), err.toString(), outputs)
  }

  override def stepInfo = {
    val dynamicInputs = inputs.collect {
      case InputData(name, data, Some(step)) => (name, step)
    }
    val staticInputs = inputs.collect {
      case InputData(name, data, None) => (name, StaticResource(data))
    }
    val allInputs = (dynamicInputs ++ staticInputs).toList
    val cmd = commandTokens.map {
      case InputFileToken(name) => s"<$name>"
      case OutputFileToken(name) => s"<$name>"
      case t => t.name
    }
    super.stepInfo
      .copy(className = "ExecuteShellCommand")
      .addParameters(allInputs: _*)
      .addParameters("cmd" -> cmd.mkString(" "))
  }
}

object ExecuteShellCommand {

  import scala.language.implicitConversions

  case class InputData(name: String, data: FlatArtifact, stepInfo: Option[PipelineStep] = None)
  case class OutputData(name: String, data: FlatArtifact)
  implicit def convertToInputData[T, A <: FlatArtifact](p: PersistedProducer[T, A]) =
    InputData(s"${p.stepInfo.className}.${p.stepInfo.signature.id}", p.artifact.asInstanceOf[FlatArtifact], Some(p))

  implicit def convertToToken(s: String) = StringToken(s)
  sealed trait CommandToken {
    def name: String
  }
  case class StringToken(name: String) extends CommandToken
  case class InputFileToken(name: String) extends CommandToken
  case class OutputFileToken(name: String) extends CommandToken

  case class CommandOutput(returnCode: Int, stdout: String, stderr: String, outputs: Iterable[OutputData])

  case class StaticResource[A <: Artifact](artifact: A) extends PipelineStep {
    override def stepInfo: PipelineStepInfo =
      PipelineStepInfo(
        className = "StaticResource",
        outputLocation = Some(artifact.url),
        parameters = Map("url" -> artifact.url.toString)
      )
  }

  case class CommandOutputComponents(stdout: Producer[String], stderr: Producer[String], outputs: Map[String, Producer[FlatArtifact]])
  def splitOutputs(shellCommand: ExecuteShellCommand): CommandOutputComponents = {
    def ifSuccessful[T](f: CommandOutput => T): () => T = { () =>
      val result = shellCommand.get
      result match {
        case CommandOutput(0, _, _, _) => f(result)
        case CommandOutput(_, _, stderr, _) =>
          sys.error(s" Failed to run command ${shellCommand.stepInfo.parameters("cmd")}: $stderr")
      }
    }
    val baseName = shellCommand.stepInfo.className
    val stdout = shellCommand.copy(create = ifSuccessful(_.stdout), stepInfo = () => shellCommand.stepInfo.copy(className = s"$baseName.stdout"))
    val stderr = shellCommand.copy(create = ifSuccessful(_.stderr), stepInfo = () => shellCommand.stepInfo.copy(className = s"$baseName.stderr"))
    val outputs =
      for (OutputData(name, data) <- shellCommand.outputs) yield {
        val outputProducer = shellCommand.copy(
          create = ifSuccessful(_.outputs.find(_.name == name).head.data),
          stepInfo = () => shellCommand.stepInfo.copy(className = s"$baseName.output.$name")
        )
        (name, outputProducer)
      }
    CommandOutputComponents(stdout, stderr, outputs.toMap)
  }
}
