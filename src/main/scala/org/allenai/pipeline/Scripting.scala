package org.allenai.pipeline

import scala.util.parsing.combinator._
import scala.annotation.switch
import scala.util._
import org.parboiled2._

/**
 * Created by michaels on 8/13/15.
 */
class Scripting {

}

// {in:train.py} -data {in:trainData.txt} -model {out:model.json}
object Scripting extends App {
  val parser = new Parser

  val code =
    """|package {source: "./scripts", id: "scripts"}
      |# Woohoo
      |{in:"$scripts/asdf"} eek {out:"$scripts/asdf"}""".stripMargin

  println(parser.parse(code))
  // (2)`
}

  class Parser extends RegexParsers {
    case class Arg(name: String, value: String)

    sealed abstract class Statement
    case class CommentStatement(comment: String) extends Statement
    case class PackageStatement(args: Seq[Arg]) extends Statement
    case class StepStatement(tokens: Seq[Token]) extends Statement

    sealed abstract class Token
    case class StringToken(value: String) extends Token
    case class ArgToken(value: Arg) extends Token

    def line = comment | packageStatement | stepStatement

    def comment = """#.*""".r ^^ { string => CommentStatement(string) }

    def packageStatement = "package {" ~ args ~ "}" ^^ { case _ ~ args ~ _ =>
      PackageStatement(args)
    }

    def stepStatement = rep(token) ^^ { case tokens => StepStatement(tokens) }
    def token: Parser[Token] = ("{" ~ arg ~ "}" ^^ { case _ ~ arg ~ _ => ArgToken(arg) } |
        ("""[^{}]+""".r ^^ { s =>
      StringToken(s)
    }))

    def args: Parser[List[Arg]] = repsep(arg, ",")
    def arg: Parser[Arg] = term ~ ":" ~ string ^^ { case term ~ ":" ~ string => Arg(term, string) }
    def string = "\"" ~> "[^\"]*".r <~ "\"" // TODO(schmmd): actually escape strings properly.
    def WS = """[ \t]*""".r
    def term: Parser[String] = """\w+""".r <~ WS

    def parseLine(s: String) = {
      parseAll(line, s)
    }

    def parse(s: String) = {
      for {
        line <- s.split("\n").toList
      } yield {
        this.parseLine(line) match {
          case Success(matchers, _) => matchers
          case fail: Failure =>
            throw new IllegalArgumentException(s"improper pattern syntax on '${line}': " + fail.msg)
          case error: Error =>
            throw new IllegalArgumentException(s"error on pattern syntax '${line}': " +
                error
                .toString)
        }
      }
    }
  }

