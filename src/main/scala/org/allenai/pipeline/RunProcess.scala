package org.allenai.pipeline

import java.io._
import java.nio.file.Files

import org.allenai.common.Resource
import org.apache.commons.io.FileUtils

import scala.io.Source

/** Executes an arbitrary system process
  * @param args   The set of tokens that comprise the command to be executed.
  * Each token is either:
  * a String
  * a Placeholder representing an input data file
  * a Placeholder representing an output data file
  * Examples:
  * StringArg("cp") InputFileArg("data.tsv") OutputFileArg("data-copy.tsv")
  * StringArg("python") InputFileArg("run.py") StringArg("-o") OutputFileArg("output.txt")
  */

class RunProcess(
  args: Seq[ProcessArg],
  stdinput: Producer[InputStream] = null,
  versionId: String = ""
)
    extends Producer[ProcessOutput] with Ai2SimpleStepInfo {
  {
    val outputFileNames = args.collect {
      case arg: OutputFileArg => arg
      case arg: OutputDirArg => arg
    }.map(_.name)
    require(outputFileNames.distinct.size == outputFileNames.size, {
      val duplicates = outputFileNames.groupBy(x => x).filter(_._2.size > 1).map(_._1)
      s"Duplicate output names: ${duplicates.mkString("[", ",", "]")}"
    })
    val inputFileNames = args.collect {
      case arg: InputFileArg => arg
      case arg: InputDirArg => arg
    }.map(_.name)
    require(inputFileNames.distinct.size == inputFileNames.size, {
      val duplicates = inputFileNames.groupBy(x => x).filter(_._2.size > 1).map(_._1)
      s"Duplicate input names: ${duplicates.mkString("[", ",", "]")}"
    })
  }

  override def create = {
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
      {
        val out = Source.fromFile(captureStdoutFile).getLines.take(100).mkString("\n")
        val err = Source.fromFile(captureStderrFile).getLines.take(100).mkString("\n")
        s"Command $command failed with status $status: ${err}\n${out}"
      }
    )

    new ProcessOutput {
      def exitStatus = status

      def stdout = new FileInputStream(captureStdoutFile)

      def stderr = new FileInputStream(captureStderrFile)

      def outputFiles = outFiles

      def outputDirs = outDirs
    }
  }

  private lazy val scratchDir = Files.createTempDirectory(null).toFile

  private val outFiles = args.collect {
    case OutputFileArg(name) =>
      val file = new File(scratchDir, name)
      (name, file)
  }.toMap

  private val outDirs = args.collect {
    case OutputDirArg(name) =>
      val file = new File(scratchDir, name)
      (name, file)
  }.toMap

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
      stepInfo = () => PipelineStepInfo("stdout").addParameters("stdout" -> outer)
    ).withCachingDisabled // Can't cache an InputStreama

  def stderr: Producer[InputStream] =
    this.copy(
      create = () => outer.get.stderr,
      stepInfo = () => PipelineStepInfo("stderr")
      .addParameters("stderr" -> outer)
    ).withCachingDisabled // Can't cache an InputStream

  def outputFiles: Map[String, Producer[File]] =
    args.collect {
      case OutputFileArg(name) =>
        (
          name,
          this.copy(
            create = () => {
            val file = outer.get.outputFiles(name)
            require(file.exists, s"Argument $name was declared as an output file, but was not created by process")
            require(!file.isDirectory, s"Argument $name was declared as an output file, but is a directory")
            file
          },
            stepInfo = () => PipelineStepInfo(name)
            .addParameters(name -> outer)
            .copy(outputLocation = Some(outer.outFiles(name).toURI))
          )
        )
    }.toMap

  def outputDirs: Map[String, Producer[File]] =
    args.collect {
      case OutputDirArg(name) =>
        (
          name,
          this.copy(
            create = () => {
            val file = outer.get.outputDirs(name)
            require(file.exists, s"Argument $name was declared as an output directory, but was not created by process")
            require(file.isDirectory, s"Argument $name was declared as an output directory, but is a file")
            file
          },
            stepInfo = () => PipelineStepInfo(name)
            .addParameters(name -> outer)
            .copy(outputLocation = Some(outer.outDirs(name).toURI))
          )
        )
    }.toMap

  def cmd(scratchDir: File): Seq[String] = {
    args.collect {
      case InputFileArg(_, p) => p.get.getCanonicalPath
      case InputDirArg(_, p) => p.get.getCanonicalPath
      case OutputFileArg(name) => new File(scratchDir, name).getCanonicalPath
      case OutputDirArg(name) => new File(scratchDir, name).getCanonicalPath
      case StringArg(arg) => arg
    }
  }

  override def stepInfo = {
    val inputFiles = for (InputFileArg(name, p) <- args) yield (name, p)
    val inputDirs = for (InputDirArg(name, p) <- args) yield (name, p)
    val stdInput = Option(stdinput).map(p => ("stdin", p)).toList
    val cmd = args.collect {
      case InputFileArg(name, p) => p.stepInfo.className
      case InputDirArg(name, p) => p.stepInfo.className
      case OutputFileArg(name) => name
      case OutputDirArg(name) => name
      case StringArg(name) => name
    }
    super.stepInfo
      .copy(
        classVersion = versionId,
        description = Some(cmd.mkString(" "))
      )
      .addParameters("cmd" -> cmd.mkString(" "))
      .addParameters(inputFiles: _*)
      .addParameters(inputDirs: _*)
      .addParameters(stdInput: _*)
  }

}

object RunProcess {
  def apply(args: ProcessArg*) = new RunProcess(args)

}

sealed trait ProcessArg {
  def name: String
}

sealed trait InputArg extends ProcessArg
sealed trait OutputArg extends ProcessArg

case class InputFileArg(name: String, inputFile: Producer[File]) extends InputArg

case class InputDirArg(name: String, inputDir: Producer[File]) extends InputArg

case class OutputFileArg(name: String) extends OutputArg

case class OutputDirArg(name: String) extends OutputArg

case class StringArg(name: String) extends ProcessArg

trait ProcessOutput {
  def exitStatus: Int

  def stdout: InputStream

  def stderr: InputStream

  def outputFiles: Map[String, File]

  def outputDirs: Map[String, File]
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

object UploadDirectory extends ArtifactIo[File, StructuredArtifact] with Ai2SimpleStepInfo {
  override def write(data: File, artifact: StructuredArtifact): Unit = {
    new DirectoryArtifact(data).copyTo(artifact)
  }

  override def read(artifact: StructuredArtifact): File = {
    artifact match {
      case dir: DirectoryArtifact => dir.dir
      case a =>
        val scratchDir = Files.createTempDirectory(null).toFile
        sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))
        val tmp = new File(scratchDir, "tmp")
        tmp.mkdir()
        a.copyTo(new DirectoryArtifact(tmp))
        tmp
    }
  }
}

case class FileInDirectory(dir: Producer[File], fileName: String) extends Producer[File] {
  override def create = new File(dir.get, fileName)
  override def stepInfo =
    PipelineStepInfo(fileName)
      .addParameters("dir" -> dir)
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

