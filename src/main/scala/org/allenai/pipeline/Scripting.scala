package org.allenai.pipeline

import scala.util.parsing.combinator._

object Scripting {
  case class Arg(name: String, value: String)

  sealed abstract class Statement
  case class CommentStatement(comment: String) extends Statement
  case class PackageStatement(args: Seq[Arg]) extends Statement
  case class StepStatement(tokens: Seq[Token]) extends Statement

  sealed abstract class Token
  case class StringToken(value: String) extends Token
  case class ArgToken(args: Seq[Arg]) extends Token

  class Parser extends RegexParsers {
    def line = comment | packageStatement | stepStatement

    def comment = """#.*""".r ^^ { string => CommentStatement(string) }

    def packageStatement = "package {" ~ args ~ "}" ^^ { case _ ~ args ~ _ =>
      PackageStatement(args)
    }

    def stepStatement = rep(token) ^^ { case tokens => StepStatement(tokens) }
    def token: Parser[Token] = argToken | stringToken
    def argToken = "{" ~ args ~ "}" ^^ { case _ ~ args ~ _ => ArgToken(args) }
    def stringToken = """[^{}]+""".r ^^ { s =>
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

    def parseLines(lines: Seq[String]) = {
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

    def parse(s: String) = {
      parseLines(s.split("\n").toList)
    }
  }
}
