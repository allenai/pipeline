package org.allenai.pipeline.hackathon

import org.allenai.pipeline._

import java.io.File
import java.net.URI

/** Model that a workflow script is parsed into */
case class WorkflowScript(
  packages: Seq[Package],
  stepCommands: Seq[StepCommand],
  outputDir: URI // this is an s3 URI
)

/** A file or directory to package up and persist in S3.
  * Primary use case is to upload a directory of scripts
  */
case class Package(id: String, source: URI)

/** A single line in a WorkflowScript that maps to a pipeline step
  *
  * Example StepCommand:
  * {{{
  * {in:$scripts/ExtractArrows.py} -i {in:./png, id:pngDir} -o {out:arrowDir, type:dir}
  * }}}
  */
case class StepCommand(tokens: Seq[CommandToken]) {
  /** Lookup output files by ID */
  def outputFiles: Map[String, CommandToken.OutputFile] = (tokens collect {
    case f: CommandToken.OutputFile => (f.id, f)
  }).toMap
}

sealed trait CommandToken
object CommandToken {
  case class Input(source: URI, id: Option[String] = None) extends CommandToken
  case class PackagedInput(id: String, path: String) extends CommandToken
  case class ReferenceInput(id: String) extends CommandToken
  case class OutputFile(id: String, suffix: String) extends CommandToken
  case class OutputDir(id: String) extends CommandToken
  case class ReferenceOutput(id: String) extends CommandToken
  case class StringToken(value: String) extends CommandToken
}
