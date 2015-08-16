package org.allenai.pipeline.hackathon

import com.typesafe.config.{ ConfigValueFactory, ConfigFactory, Config }
import scala.collection.mutable.{ Map => MMap }

import java.net.URI

import org.allenai.pipeline.hackathon
import org.allenai.pipeline.hackathon.CommandToken._

import scala.util.parsing.combinator._

class PipescriptCompiler() {
  protected[this] val parser = new PipescriptParser.Parser

  def compile(statements: TraversableOnce[PipescriptParser.Statement]): Pipescript = {
    var packages = Vector.empty[hackathon.Package]
    var stepCommands = Vector.empty[hackathon.StepCommand]

    var environment = MMap.empty[String, String]
    statements.map(_.resolve(environment)) foreach {
      case PipescriptParser.CommentStatement(_) =>
      // The call to resolve above has already modified the environment
      case PipescriptParser.SetStatement(block) =>
      case PipescriptParser.PackageStatement(block) =>
        val source = block.findGet("source")
        val id = block.findGet("id")
        val sourceUri = new URI(source)
        packages :+= hackathon.Package(id, sourceUri)
      case PipescriptParser.StepStatement(tokens) =>
        stepCommands :+= StepCommand(tokens.map(transformToken))
    }

    def transformToken(scriptToken: PipescriptParser.Token): CommandToken = {
      scriptToken match {
        case PipescriptParser.StringToken(s) => CommandToken.StringToken(s)
        case t @ PipescriptParser.ArgToken(block) =>
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

    Pipescript(packages, stepCommands)
  }

  def compileScript(text: String): Pipescript = {
    this.compile(parser.parseScript(text))
  }
}
