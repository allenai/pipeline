package org.allenai.pipeline

import java.io.{ ByteArrayInputStream, File, FileWriter, InputStream }
import java.nio.file.Files
import java.util.UUID

import org.allenai.common.Resource
import org.allenai.pipeline.ExternalProcess._
import org.allenai.pipeline.IoHelpers._
import org.apache.commons.io.{ FileUtils, IOUtils }

import scala.annotation.tailrec
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
    val scratchDir = Files.createTempDirectory(null).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    val ab = argsBound(inputs, commandTokens).toList

    import scala.sys.process._
    val captureStdoutFile = new File(scratchDir, "stdout")
    val captureStderrFile = new File(scratchDir, "stderr")
    val out = new FileWriter(captureStdoutFile)
    val err = new FileWriter(captureStderrFile)

    val logger = ProcessLogger(
      (o: String) => out.append(o),
      (e: String) => err.append(e)
    )

    prepareInputPaths(ab, scratchDir)
    val cmd1 : List[String] = cmd(ab, scratchDir)

    val status = (cmd1 #< stdinput()) ! logger
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

  /** A string provided by the ExternalProcess template.  Does not solicit
    * a value at run().
    */
  case class StringToken(name: String) extends CommandToken

  /** An ExternalProcess input artifact, specified when applying a
    * RunExternalProcess template, or at ExternalProcess.run.  Solicits
    * a value at run().
    */
  case class InputFileToken(name: String) extends CommandToken

  /** An ExternalProcess output artifact, specified when applying a
    * RunExternalProcess template, or at ExternalProcess.run.  Solicits
    * a value at run().
    */
  case class OutputFileToken(name: String) extends CommandToken

  /** Specify the location of an ExternalProcess entry point, using a file
    * path.  We recommend the file path to be absolute.  RunExternalProcess
    * provides only the .getName portion of a ScriptToken's absolute path
    * to the pipeline when declaring the signature of the RunExternalProcess.
    * ScriptToken enables producers following a RunExternalProcess to retrieve a
    * cached artifact on a machine other than the machine that recorded the cached
    * artifact.  If the entire absolute path were submitted, it would cause the
    * signature to hash spurious information.  For example, if a certain
    * pipeline was being change tracked in a source directory under a user's
    * home directory, then no other user could exchange cached data without
    * being logged on some machine with the same home directory.
    *
    * Does not solicit a value at run().
    */
  case class ScriptToken(abspath: String) extends CommandToken {
    override def name: String = {
      val f : File = new File(abspath)
      f.getName  // the .getName of "/usr/bin/perl" is "perl".  Like unix "basename"
    }
  }
  
  /** Things which ExternalProcess is prepared to consume, currently
    * only ExtargStream */
  sealed trait Extarg

  case class ExtargStream(a : Producer[() => InputStream]) extends Extarg

  class ExternalProcessArgException(s : String) extends Throwable

  /** The policy for assiging Extargs to CommandTokens. */
  def joinArgsBound(
                     commandTokens: Seq[CommandToken],
                     inputs: Seq[Extarg]) : Seq[(CommandToken, Option[Extarg])] = {
    @tailrec
    def iter(commandTokens: Seq[CommandToken],
             inputs: Seq[Extarg],
             c : Seq[(CommandToken, Option[Extarg])]) : Seq[(CommandToken, Option[Extarg])] = {
      (commandTokens, inputs) match {
        case ((tok: InputFileToken) :: tlA,
              ExtargStream(st)      :: tlB) =>     iter(tlA, tlB, (tok, Some(ExtargStream(st))) +: c)
        case ((tok: OutputFileToken) :: tlA, _) => iter(tlA, inputs, (tok, None) +: c)
        case ((tok: StringToken) :: tlA, _) =>     iter(tlA, inputs, (tok, None) +: c)
        case (hdA :: tlA, _) =>                    iter(tlA, inputs, (hdA, None) +: c)
        case (Nil, hdB :: tlB) =>                  c.reverse
          //TODO: require(false, s"too many arguments supplied"); c
        case (Nil, Nil) =>                         c.reverse
        case (_, _) =>                             c.reverse
      }
    }
    iter(commandTokens, inputs, Seq())
  }

  /** Translate joinArgsBound into a contributed string for the final command, but stop short
    * of nondeterministic components which could disrupt signature stability, such as the
    * temporary directory prefix. */
  def argsBound(inputs: Seq[Extarg], commandTokens: Seq[CommandToken]) :
      List[(CommandToken, Option[Extarg], String)] =
  {
      val jab = joinArgsBound(commandTokens.toList, inputs.toList).toList
      val paths : List[String] = jab.zipWithIndex.map({
        case ((tok:CommandToken, argopt:Option[Extarg]), i) =>
          ((tok, argopt), i) match {
            case ((StringToken(s), arg), i) => s
            case ((InputFileToken(nameTemp1), Some(ExtargStream(b))), i) => nameAuto(i)
            case ((OutputFileToken(nameTemp1), _), i) => if(nameTemp1 != null) nameTemp1 else nameAuto(i)
            case ((ScriptToken(path), _), i) => path
            case (_,i) =>
              throw new ExternalProcessArgException(s"Type mismatch on argument $i")
          }
      })
      jab.zip(paths).map({case ((a,b),c) => (a,b,c)})
  }

  /** Default naming for temporary files holding input and output streams. */
  private def nameAuto(i:Int) = f"tmp_${i}"

  /** Write input ExtargStream arguments to temporary files */
  def prepareInputPaths(args:List[(CommandToken, Option[Extarg], String)], scratchDir:File): Unit = {
    args.zipWithIndex.foreach{ x : ((CommandToken, Option[Extarg], String),Int) =>
      x match {
        case ((InputFileToken(nameTemp), Some(ExtargStream(b)), name), i) =>
          val f = new File(scratchDir, name)
          StreamIo.write(b.get, new FileArtifact(f))
        case ((StringToken(s), _, _), i) =>
        case ((OutputFileToken(nameTemp), _, _), i) =>
        case (_, i) =>
      }
    }
  }

  /** Translate every file hintname into a temporary directory path.  Return the final command. */
  def cmd(args:List[(CommandToken, Option[Extarg], String)], scratchDir:File) : List[String] = {
    args.map {
      case (InputFileToken(_),_,path) => new File(scratchDir,path).getCanonicalPath
      case (OutputFileToken(name),_,path) => new File(scratchDir,path).getCanonicalPath
      case (StringToken(t),_,_) => t
      case (ScriptToken(path),_,_) => path
    }
  }

  import scala.language.implicitConversions

  /* TODO: pipeline.persist to ExternalProcess */
  implicit def convertPersistedProducer1ToInputData[A <: FlatArtifact](p: PersistedProducer[() => InputStream, A]): ExtargStream = {
    ExtargStream(p.copy(create =
      () => {
        p.get
        StreamIo.read(p.artifact.asInstanceOf[FlatArtifact])
      }))
  }

  implicit def convertPersistedProducer2ToInputData[T, A <: FlatArtifact](p: PersistedProducer[T, A]): ExtargStream = {
    ExtargStream(p.copy(create =
      () => {
        p.get
        StreamIo.read(p.artifact.asInstanceOf[FlatArtifact])
      }))
  }

  implicit def convertProducerToInputData(p: Producer[() => InputStream]): Extarg = {
    ExtargStream(p)
  }

  implicit def convertArtifactToInputData[A <: FlatArtifact](artifact: A): Extarg = {
    ExtargStream(DynamicResource(artifact))
  }

  implicit def convertToToken(s: String): StringToken = StringToken(s)

  case class CommandOutput(
    returnCode: Int,
    stdout: () => InputStream,
    stderr: () => InputStream,
    outputs: Map[String, () => InputStream]
  )
}

/** An ExternalProcess wrapped with "Producer"ness on inputs and outputs.  A related
  * pipeline convention: Name Producer subclasses with a verb. */
class RunExternalProcess private (
    val commandTokens: Seq[CommandToken],
    _versionHistory: Seq[String],
    inputs: Seq[Extarg]
) extends Producer[CommandOutput] with Ai2SimpleStepInfo {
    override def create = {
      // side effect: generate demand
      inputs.foreach({
        case ExtargStream(v) => v.get
      })
      new ExternalProcess(commandTokens: _*).run(inputs)
    }

  override def versionHistory = _versionHistory

  override def stepInfo = {
    val ab = argsBound(inputs, commandTokens)
    val cmd = commandTokens.map {
      case InputFileToken(name) => s"<$name>"
      case OutputFileToken(name) => s"<$name>"
      case t => t.name
    }
    val dep : Map[String, Producer[_]] = ab.toList.flatMap({
        case (InputFileToken(p),Some(ExtargStream(prod)),n) => List((n,prod))
        case _ => List()
        // StringInputToken is not a depenency, just like the code of a Producer
        // implemented in Scala is not a depenency, since the hashing of either
        // would make cache hits undesirably rare.  Instead, provide semantic
        // versioning using .versionHistory.
      }).toMap
    super.stepInfo
      .copy(
        className = "ExternalProcess",
        dependencies = dep)
      .addParameters("cmd" -> cmd.mkString(" "))
      // TODO: make an Extarg for string parameters and report with .addParameters
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
    val processCmd = new RunExternalProcess(commandTokens, versionHistory, inputs)
    val baseName = processCmd.stepInfo.className
    val stdout = new ExtractOutputComponent("stdout", _.stdout, processCmd, requireStatusCode)
    val stderr = new ExtractOutputComponent("stderr", _.stderr, processCmd, requireStatusCode)
    val outputStreams =
      for ((_,_,name) <- argsBound(inputs, commandTokens)) yield {
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

    override def stepInfo: PipelineStepInfo = {
      PipelineStepInfo(className = name)
        .addParameters("cmd" -> processCmd)
    }

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

