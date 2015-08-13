package org.allenai.pipeline.hackathon

import org.allenai.pipeline._

import java.io.File
import java.net.URI

/** Model that a workflow script is parsed into */
case class WorkflowScript(
  packages: Seq[Package],
  commands: Seq[StepCommand]
)

/** A file or directory to package up and persist in S3.
  * Primary use case is to upload a directory of scripts
  */
case class Package(id: String, location: File)

/** A single line in a WorkflowScript that maps to a pipeline step
  *
  * For example:
  * {{{
  * {in:$scripts/ExtractArrows.py} -i {in:./png, id:pngDir} -o {out:arrowDir, type:dir}
  * }}}
  */
case class StepCommand(tokens: Seq[CommandToken])

sealed trait CommandToken
object CommandToken {
  case class Input(source: URI, id: Option[String]) extends CommandToken
  case class Output(id: String, outputType: OutputType) extends CommandToken
  case class String(value: String) extends CommandToken
}

sealed trait OutputType
object OutputType {
  case object Dir extends OutputType
  case object Json extends OutputType
  case object Text extends OutputType
}
