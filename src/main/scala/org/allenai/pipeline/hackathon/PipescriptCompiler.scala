package org.allenai.pipeline.hackathon

import com.typesafe.config.{ ConfigValueFactory, ConfigFactory, Config }

import java.net.URI

import org.allenai.pipeline.hackathon
import org.allenai.pipeline.hackathon.CommandToken._

import scala.util.parsing.combinator._

class PipescriptCompiler() {
  protected[this] val parser = new PipescriptParser.Parser

  def parseStatements(outputDir: URI)(parsedStatements: TraversableOnce[PipescriptParser.Statement]): WorkflowScript = {
    var packages = Vector.empty[hackathon.Package]
    var stepCommands = Vector.empty[hackathon.StepCommand]

    var environment = Map.empty[String, String]
    parsedStatements.foreach {
      case PipescriptParser.CommentStatement(_) =>
      case PipescriptParser.VariableStatement(name, value) =>
        environment += (name -> value.resolve(environment))
      case PipescriptParser.PackageStatement(block) =>
        val source = block.findGet("source").resolve(environment)
        val id = block.findGet("id").resolve(environment)
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
            block.find("package").map(_.resolve(environment)).map {
              pkgName => PackagedInput(pkgName, block.findGet("file").resolve(environment))
            }.getOrElse(sys.error(s"'file' without 'package' in $t"))
          } else if (block.hasKey("input")) {
            val url = new URI(block.findGet("input").resolve(environment))
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
              OutputDir(block.findGet("output").resolve(environment))
            } else {
              OutputFile(
                block.findGet("output").resolve(environment),
                block.find("suffix").map(_.resolve(environment)).getOrElse("")
              )
            }
          } else if (block.find("ref").nonEmpty) {
            ReferenceOutput(block.findGet("ref").resolve(environment))
          } else {
            throw new IllegalArgumentException("This should not happen!")
          }
      }
    }

    WorkflowScript(packages, stepCommands, outputDir)
  }

  def parseLines(outputDir: URI)(lines: TraversableOnce[String]): WorkflowScript = {
    this.parseStatements(outputDir)(parser.parseLines(lines))
  }

  def parseText(outputDir: URI)(text: String): WorkflowScript = {
    this.parseStatements(outputDir)(parser.parseText(text))
  }
}
