package org.allenai.pipeline

import java.net.URI

import org.allenai.pipeline.PipeScript._
import org.allenai.pipeline.PipeScript.CommandToken._

import scala.collection.mutable.{ Map => MMap }

/** Produces a PipeScript object from a sequence of parsed statements,
  * with all variable references resolved
  */
object PipeScriptCompiler {

  def compile(statements: TraversableOnce[PipeScriptParser.Statement]): PipeScript = {
    var packages = Vector.empty[Package]
    var stepCommands = Vector.empty[RunCommand]

    var environment = MMap.empty[String, String]
    statements.map(_.resolve(environment)) foreach {
      // The call to resolve above has already modified the environment
      case PipeScriptParser.SetStatement(block) =>
      case PipeScriptParser.PackageStatement(block) =>
        val source = block.findGet("source")
        val id = block.findGet("id")
        val sourceUri = new URI(source)
        packages :+= Package(id, sourceUri)
      case PipeScriptParser.RunStatement(tokens) =>
        stepCommands :+= RunCommand(tokens.map(transformToken))
    }

    def transformToken(scriptToken: PipeScriptParser.Token): CommandToken = {
      scriptToken match {
        case PipeScriptParser.StringToken(s) => CommandToken.StringToken(s.asString)
        case t @ PipeScriptParser.KeyValuePairsToken(block) =>
          if (block.hasKey("file")) {
            block.find("package").map {
              pkgName => PackagedInput(pkgName, block.findGet("file"))
            }.getOrElse(sys.error(s"'file' without 'package' in $t"))
          } else if (block.hasKey("input")) {
            val url = new URI(block.findGet("input"))
            val isDir = block.find("type").exists(_ == "dir")
            val isUrl = block.find("type").exists(_ == "url")
            if (isDir) {
              CommandToken.InputDir(url)
            } else if (isUrl) {
              CommandToken.InputUrl(url)
            } else {
              CommandToken.InputFile(url)
            }
          } else if (block.hasKey("output")) {
            if (block.find("type").exists(_ == "dir")) {
              OutputDir(block.findGet("output"))
            } else {
              OutputFile(
                block.findGet("output"),
                block.find("suffix").getOrElse("")
              )
            }
          } else if (block.find("ref").nonEmpty) {
            ReferenceOutput(block.findGet("ref"))
          } else {
            throw new IllegalArgumentException("This should not happen!")
          }
      }
    }

    PipeScript(packages, stepCommands)
  }

  def compileScript(text: String): PipeScript = {
    this.compile(new PipeScriptParser.Parser().parseScript(text))
  }
}
