package org.allenai.pipeline.hackathon

import java.net.URI

import org.allenai.pipeline.hackathon
import org.allenai.pipeline.hackathon.CommandToken._

import scala.util.parsing.combinator._

class PipelineScriptParser() {
  protected[this] val parser = new PipelineScript.Parser

  def parseStatements(outputDir: URI)(parsedStatements: TraversableOnce[PipelineScript.Statement]): WorkflowScript = {
    var packages = Vector.empty[hackathon.Package]
    var stepCommands = Vector.empty[hackathon.StepCommand]

    parsedStatements.foreach {
      case PipelineScript.CommentStatement(_) =>
      case PipelineScript.PackageStatement(args) =>
        val source = args.find(_.name == "source").getOrElse {
          throw new IllegalArgumentException("No argument 'source' in package: " + args)
        }
        val id = args.find(_.name == "id").getOrElse {
          throw new IllegalArgumentException("No argument 'id' in package: " + args)
        }
        val sourceUri = new URI(source.value)
        packages :+= hackathon.Package(id.value, sourceUri)
      case PipelineScript.StepStatement(tokens) =>
        stepCommands :+= StepCommand(tokens.map(transformToken))
    }

    WorkflowScript(packages, stepCommands, outputDir)
  }

  def transformToken(scriptToken: PipelineScript.Token): CommandToken = {
    scriptToken match {
      case PipelineScript.StringToken(s) => CommandToken.StringToken(s)
      case t @ PipelineScript.ArgToken(args) if t.find("file").nonEmpty =>
        t.find("package").map {
          pkgName => PackagedInput(pkgName, t.find("file").get)
        }.getOrElse(sys.error(s"'file' without 'package' in $t"))
      case t @ PipelineScript.ArgToken(args) if t.find("upload").nonEmpty =>
        val url = new URI(t.find("upload").get)
        val isDir = t.find("type").exists(_ == "dir")
        if (isDir) {
          CommandToken.InputDir(url)
        } else {
          CommandToken.InputFile(url)
        }

      case t @ PipelineScript.ArgToken(args) if t.find("out").nonEmpty =>
        if (t.find("type").exists(_ == "dir")) {
          OutputDir(t.find("out").get)
        } else {
          OutputFile(t.find("out").get, t.find("suffix").getOrElse(""))
        }
      case t @ PipelineScript.ArgToken(args) if t.find("upload").nonEmpty =>
        if (t.find("type").exists(_ == "dir")) {
          OutputDir(t.find("out").get)
        } else {
          OutputFile(t.find("out").get, t.find("suffix").getOrElse(""))
        }
      case t @ PipelineScript.ArgToken(args) if t.find("ref").nonEmpty =>
        ReferenceOutput(t.find("ref").get)
    }
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
  case class PackageStatement(args: Seq[Arg]) extends Statement
  case class StepStatement(tokens: Seq[Token]) extends Statement

  sealed abstract class Token
  case class StringToken(value: String) extends Token
  case class ArgToken(args: Seq[Arg]) extends Token {
    def find(argName: String) = args.find(_.name == argName).map(_.value)
  }

  class Parser extends RegexParsers {
    def line = comment | packageStatement | stepStatement

    def comment = """#.*""".r ^^ { string => CommentStatement(string) }

    def packageStatement = "package {" ~ args ~ "}" ^^ {
      case _ ~ args ~ _ =>
        PackageStatement(args)
    }

    def stepStatement = rep(token) ^^ { case tokens => StepStatement(tokens) }
    def token: Parser[Token] = argToken | stringToken
    def argToken = "{" ~ args ~ "}" ^^ { case _ ~ args ~ _ => ArgToken(args) }
    def stringToken = """[^{}\s]+""".r ^^ { s =>
      StringToken(s)
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
          case fail: Failure =>
            throw new IllegalArgumentException(s"improper pattern syntax on '${line}': " + fail.msg)
          case error: Error =>
            throw new IllegalArgumentException(s"error on pattern syntax '${line}': " +
              error.toString)
        }
      }
    }

    def parseText(s: String) = {
      parseLines(s.split("\n").toList)
    }
  }
}
