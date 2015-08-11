package org.allenai.pipeline

import org.allenai.common.Resource

import org.apache.commons.io.FileUtils

import scala.io.Source

import java.io._
import java.nio.file.Files

/** Executes an arbitrary system process
  * @param args   The set of tokens that comprise the command to be executed.
  *               Each token is either:
  *               a String
  *               a Placeholder representing an input data file
  *               a Placeholder representing an output data file
  *               Examples:
  *               StringArg("cp") InputFileArg("data.tsv") OutputFileArg("data-copy.tsv")
  *               StringArg("python") InputFileArg("run.py") StringArg("-o") OutputFileArg("output.txt")
  */

class RunProcess(
  args: Seq[ProcessArg],
  stdinput: Producer[InputStream] = null,
  versionId: String = ""
)
    extends Producer[ProcessOutput] with Ai2SimpleStepInfo {
  {
    val outputFileNames = args.collect { case arg: OutputFileArg => arg }.map(_.name)
    require(outputFileNames.distinct.size == outputFileNames.size, {
      val duplicates = outputFileNames.groupBy(x => x).filter(_._2.size > 1).map(_._1)
      s"Duplicate output names: ${duplicates.mkString("[", ",", "]")}"
    })
  }

  override def create = {
    val scratchDir = Files.createTempDirectory(null).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    import scala.sys.process._
    val captureStdoutFile = new File(scratchDir, "stdout")
    val captureStderrFile = new File(scratchDir, "stderr")
    val stdout = new FileWriter(captureStdoutFile)
    val stderr = new FileWriter(captureStderrFile)
    val inputStream = Option(stdinput).map(_.get).getOrElse(new ByteArrayInputStream(Array.emptyByteArray))

    val logger = ProcessLogger(
      (o: String) => stdout.append(o).append('\n'),
      (e: String) => stderr.append(e).append('\n')
    )

    val command = cmd(scratchDir)

    val status = (command #< inputStream) ! logger
    stdout.close()
    stderr.close()

    require(
      requireStatusCode.contains(status),
      s"Command $command failed with status $status: ${Source.fromFile(captureStderrFile).getLines.take(100).mkString("\n")}"
    )

    val outFiles = args.collect {
      case OutputFileArg(name) =>
        val file = new File(scratchDir, name)
        require(file.exists, s"Argument $name was declared as an output file, but was not created by process")
        (name, file)
    }.toMap

    new ProcessOutput {
      def exitStatus = status

      def stdout = new FileInputStream(captureStdoutFile)

      def stderr = new FileInputStream(captureStderrFile)

      def outputFiles = outFiles
    }
  }

  // Specify an input stream that will be fed as stdin to the process
  def withStdin(stdinput: Producer[InputStream]) =
    new RunProcess(args, stdinput, versionId)

  // Specify a versionId for the behavior of the external process
  // For example, if the process calls a script in the user's PATH, then
  // RunProcess should be instantiated with a different versionId when
  // the logic of the underlying script changes
  def withVersionId(id: String) =
    new RunProcess(args, stdinput, id)

  // Fail if the external process returns with a status code not included in this set
  def requireStatusCode = Set(0)

  val outer = this

  def stdout: Producer[InputStream] =
    this.copy(
      create = () => outer.get.stdout,
      stepInfo = () => PipelineStepInfo("stdout")
      .addParameters("cmd" -> outer)
    ).withCachingDisabled // Can't cache an InputStreama

  def stderr: Producer[InputStream] =
    this.copy(
      create = () => outer.get.stderr,
      stepInfo = () => PipelineStepInfo("stderr")
      .addParameters("cmd" -> outer)
    ).withCachingDisabled // Can't cache an InputStream

  def outputFiles: Map[String, Producer[File]] =
    args.collect {
      case OutputFileArg(name) =>
        (
          name,
          this.copy(
            create = () => outer.get.outputFiles(name),
            stepInfo = () => PipelineStepInfo(name)
            .addParameters("cmd" -> outer)
            .copy(outputLocation = Some(outer.get.outputFiles(name).toURI))
          )
        )
    }.toMap

  def cmd(scratchDir: File): Seq[String] = {
    args.collect {
      case InputFileArg(_, p) => p.get.getCanonicalPath
      case OutputFileArg(name) => new File(scratchDir, name).getCanonicalPath
      case StringArg(arg) => arg
    }
  }

  override def stepInfo = {
    val inputFiles = for (InputFileArg(name, p) <- args) yield (name, p)
    val stdInput = Option(stdinput).map(p => ("stdin", p)).toList
    val cmd = args.collect {
      case InputFileArg(name, _) => s"<$name>"
      case OutputFileArg(name) => s"<$name>"
      case StringArg(name) => name
    }
    super.stepInfo
      .copy(classVersion = versionId)
      .addParameters("cmd" -> cmd.mkString(" "))
      .addParameters(inputFiles: _*)
      .addParameters(stdInput: _*)
  }

}

object RunProcess {
  def apply(args: ProcessArg*) = new RunProcess(args)
}

trait ProcessArg {
  def name: String
}

case class InputFileArg(name: String, inputFile: Producer[File]) extends ProcessArg

case class OutputFileArg(name: String) extends ProcessArg

case class StringArg(name: String) extends ProcessArg

trait ProcessOutput {
  def exitStatus: Int

  def stdout: InputStream

  def stderr: InputStream

  def outputFiles: Map[String, File]
}

object CopyFlatArtifact extends ArtifactIo[FlatArtifact, FlatArtifact] with Ai2SimpleStepInfo {
  override def write(data: FlatArtifact, artifact: FlatArtifact): Unit =
    data.copyTo(artifact)

  override def read(artifact: FlatArtifact): FlatArtifact = artifact
}

object UploadFile extends ArtifactIo[File, FlatArtifact] with Ai2SimpleStepInfo {
  override def write(data: File, artifact: FlatArtifact): Unit =
    new FileArtifact(data).copyTo(artifact)

  override def read(artifact: FlatArtifact): File = {
    artifact match {
      case f: FileArtifact => f.file
      case a =>
        val scratchDir = Files.createTempDirectory(null).toFile
        sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))
        val tmp = new File(scratchDir, "tmp")
        a.copyTo(new FileArtifact(tmp))
        tmp
    }
  }
}

object SaveStream extends ArtifactIo[InputStream, FlatArtifact] with Ai2SimpleStepInfo {
  override def write(data: InputStream, artifact: FlatArtifact): Unit = {
    artifact.write { writer =>
      val BUFFER_SIZE = 16384
      val buffer = new Array[Byte](BUFFER_SIZE)
      Resource.using(data) { is =>
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
          writer.write(buffer, 0, n))
      }
    }
  }

  override def read(artifact: FlatArtifact): InputStream = artifact.read
}

