package org.allenai.pipeline

import java.net.URI

/** A fully parsed script */
case class PipeScript(
    packages: Seq[PipeScript.Package],
    runCommands: Seq[PipeScript.RunCommand]
) {

  /** Text that can be parsed into this script.
    * May not be identical to the text this script was originally parsed from
    * Formatting will not be preserved, and variables will be resolved
    * @return
    */
  def scriptText = {
    val buffer = new StringBuilder
    packages foreach { pkg =>
      buffer.append(pkg.scriptText).append("\n")
    }
    buffer.append("\n")
    runCommands foreach { step =>
      buffer.append(step.scriptText).append("\n")
    }
    buffer.mkString
  }
}

object PipeScript {
  case class Package(id: String, source: URI) {
    def scriptText: String = s"""package {id:"${id}", source:"${source}"}"""
  }

  /** A single line in a WorkflowScript that maps to a pipeline step
    *
    * Example StepCommand:
    * {{{
    * {in:$scripts/ExtractArrows.py} -i {in:./png, id:pngDir} -o {out:arrowDir, type:dir}
    * }}}
    */
  case class RunCommand(tokens: Seq[CommandToken]) {
    /** Lookup output files by ID */
    def outputFiles: Map[String, CommandToken.OutputFile] = (tokens collect {
      case f: CommandToken.OutputFile => (f.id, f)
    }).toMap
    def scriptText = s"""run ${tokens.map(_.scriptText).mkString(" ")}"""
  }

  sealed trait CommandToken {
    def scriptText: String
  }
  object CommandToken {

    /** A directory input
      * @param source
      */
    case class InputDir(source: URI) extends CommandToken {
      def scriptText: String = s"""{input:"$source", type:"dir"}"""
    }

    /** A file input
      * @param source
      */
    case class InputFile(source: URI) extends CommandToken {
      def scriptText: String = s"""{input:"$source", type:"file"}"""
    }

    /** A URL input
      * @param source
      */
    case class InputUrl(source: URI) extends CommandToken {
      def scriptText: String = s"""{input:"$source", type:"url"}"""
    }

    /** A file that exists in a Package
      * @param id the package's ID
      * @param path relative path from the package
      */
    case class PackagedInput(id: String, path: String) extends CommandToken {
      def scriptText: String = s"""{file:"$path", package:"$id"}"""
    }

    /** An input that is a reference to an output declared in an upstream step
      * @param id the id given to the upstream output
      */
    case class ReferenceOutput(id: String) extends CommandToken {
      def scriptText: String = s"""{ref:"$id"}"""
    }

    /** A file to output
      * @param id id to use as a reference in downstream steps
      * @param suffix will determine the content type
      */
    case class OutputFile(id: String, suffix: String) extends CommandToken {
      def scriptText: String = s"""{output:"$id", type:"file", suffix:"$suffix"}"""
    }

    /** A directory to output
      * @param id id to use as a reference in downstream steps
      */
    case class OutputDir(id: String) extends CommandToken {
      def scriptText: String = s"""{output:"$id", type:"dir"}"""
    }

    /** An arbitrary string */
    case class StringToken(value: String) extends CommandToken {
      def scriptText: String = value
    }

  }
}
