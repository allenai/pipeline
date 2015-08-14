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
  /** A directory input
    * @param source
    * @param id if set will be referencable by subsequent steps
    */
  case class InputDir(source: URI, id: Option[String] = None) extends CommandToken

  /** A file input
    * @param source
    * @param id if set will be referencable by subsequent steps
    */
  case class InputFile(source: URI, id: Option[String] = None) extends CommandToken

  /** A file that exists in a Package
    * @param id the package's ID
    * @param path relative path from the package
    */
  case class PackagedInput(id: String, path: String) extends CommandToken

  /** An input that is a reference to an input declared in an upstream step
    * @param id the id given to the upstream input
    */
  case class ReferenceInput(id: String) extends CommandToken

  /** An input that is a reference to an output declared in an upstream step
    * @param id the id given to the upstream output
    */
  case class ReferenceOutput(id: String) extends CommandToken

  /** A file to output
    * @param id id to use as a reference in downstream steps
    * @param suffix will determine the content type
    */
  case class OutputFile(id: String, suffix: String) extends CommandToken

  /** A directory to output
    * @param id id to use as a reference in downstream steps
    */
  case class OutputDir(id: String) extends CommandToken

  /** An arbitrary string */
  case class StringToken(value: String) extends CommandToken
}
