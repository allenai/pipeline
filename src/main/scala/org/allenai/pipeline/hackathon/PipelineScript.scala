package org.allenai.pipeline.hackathon

import com.typesafe.config.{ConfigValueFactory, ConfigFactory, Config}

import java.net.URI

import org.allenai.pipeline.hackathon
import org.allenai.pipeline.hackathon.CommandToken._

import scala.util.parsing.combinator._

class PipelineScriptParser() {
  protected[this] val parser = new PipelineScript.Parser

  def parseStatements(outputDir: URI)(parsedStatements: TraversableOnce[PipelineScript.Statement]): WorkflowScript = {
    var packages = Vector.empty[hackathon.Package]
    var stepCommands = Vector.empty[hackathon.StepCommand]

    var environment = ConfigFactory.empty
    parsedStatements.foreach {
      case PipelineScript.CommentStatement(_) =>
      case PipelineScript.VariableStatement(name, value) =>
        val resolvedValue = ConfigFactory.parseString("v=" + value).getString("v")
        environment = environment.withValue(name, ConfigValueFactory.fromAnyRef(resolvedValue))
      case PipelineScript.PackageStatement(block) =>
        val source = block.resolve(environment).getString("source")
        val id = block.resolve(environment).getString("id")
        val sourceUri = new URI(source)
        packages :+= hackathon.Package(id, sourceUri)
      case PipelineScript.StepStatement(tokens) =>
        stepCommands :+= StepCommand(tokens.map(transformToken))
    }

    def transformToken(scriptToken: PipelineScript.Token): CommandToken = {
      scriptToken match {
        case PipelineScript.StringToken(s) => CommandToken.StringToken(s)
        case t@PipelineScript.ArgToken(block) =>
          val resolved = block.resolve(environment)
          def find(key: String) = {
            if (resolved.hasPath(key)) {
              Some(resolved.getString(key))
            }
            else {
              None
            }
          }
          if (resolved.hasPath("file")) {
            find("package").map {
              pkgName => PackagedInput(pkgName, find("file").get)
            }.getOrElse(sys.error(s"'file' without 'package' in $t"))
          } else if (find("upload").nonEmpty) {
            val url = new URI(find("upload").get)
            val isDir = find("type").exists(_ == "dir")
            val isUrl = find("type").exists(_ == "url")
            if (isDir) {
              CommandToken.InputDir(url)
            } else if (isUrl) {
              CommandToken.InputUrl(url)
            }
            else {
              CommandToken.InputFile(url)
            }
          }
          else if (find("out").nonEmpty) {
            if (find("type").exists(_ == "dir")) {
              OutputDir(find("out").get)
            } else {
              OutputFile(find("out").get, find("suffix").getOrElse(""))
            }
          }
          else if (find("ref").nonEmpty) {
            ReferenceOutput(find("ref").get)
          }
          else {
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

object PipelineScript {

  case class Arg(name: String, value: String)

  sealed abstract class Statement

  case class CommentStatement(comment: String) extends Statement

  case class VariableStatement(name: String, value: String) extends Statement

  case class PackageStatement(block: Block) extends Statement

  case class StepStatement(tokens: Seq[Token]) extends Statement

  case class Block(value: String) {
    def resolve(context: Config): Config = {
      ConfigFactory.parseString(value).resolveWith(context)
    }
  }

  sealed abstract class Token

  case class StringToken(value: String) extends Token

  case class ArgToken(block: Block) extends Token

  class Parser extends RegexParsers {
    def line = comment | packageStatement | variableStatement | stepStatement

    def comment = """#.*""".r ^^ { string => CommentStatement(string) }

    def packageStatement = "package" ~ block ^^ { case _ ~ b =>
      PackageStatement(b)
    }

    def stepStatement = rep(token) ^^ { case tokens => StepStatement(tokens) }

    def token: Parser[Token] = argToken | stringToken

    def argToken = block ^^ { block => ArgToken(block) }

    def stringToken = """[^{}\s]+""".r ^^ { s =>
      StringToken(s)
    }

    def variableStatement = "set" ~> term ~ "=" ~ """.*""".r ^^ { case name ~ _ ~ value =>
      VariableStatement(name, value)
    }

    def block: Parser[Block] = blockText ^^ {
      Block(_)
    }

    def blockText: Parser[String] = "{" ~ "[^{}]*".r ~ rep(blockText) ~ "[^{}]*".r ~ "}" ^^ {
      case "{" ~ left ~ subblocks ~ right ~ "}" => "{" + left + subblocks.mkString("") + right + "}"
    }

    def args: Parser[List[Arg]] = repsep(arg, ",")

    def arg: Parser[Arg] = term ~ ":" ~ string ^^ { case term ~ ":" ~ string => Arg(term, string) }

    def string = "\"" ~> "[^\"]*".r <~ "\""

    // TODO(schmmd): actually escape strings properly.
    def WS = """[ \t]*""".r

    def term: Parser[String] = """\w+""".r <~ WS

    def parseLine(s: String) = {
      parseAll(line, s)
    }

    def parseLines(lines: TraversableOnce[String]): TraversableOnce[PipelineScript.Statement] = {
      for {
        line <- lines
        if !line.trim.isEmpty
      } yield {
        this.parseLine(line) match {
          case Success(matchers, _) => matchers
          case e: NoSuccess =>
            Console.err.println(e)
            throw new IllegalArgumentException(s"failed to parse '${line}'")
        }
      }
    }

    def parseText(s: String) = {
      parseLines(s.split("\n").toList)
    }
  }

}
