package org.allenai.pipeline.hackathon

import scala.util.parsing.combinator._

object PipescriptParser {
  sealed abstract class Statement
  case class CommentStatement(comment: String) extends Statement
  case class VariableStatement(name: String, value: Value) extends Statement
  case class PackageStatement(block: Block) extends Statement
  case class StepStatement(tokens: Seq[Token]) extends Statement

  case class Block(args: Seq[Arg]) {
    def hasKey(name: String): Boolean = {
      args.exists(_.name == name)
    }

    def find(name: String): Option[String] = {
      args.find(_.name == name).map(_.value)
    }

    def findGet(name: String): String = {
      args.find(_.name == name).getOrElse {
        throw new IllegalArgumentException(s"Could not find key '${name}' in arguments: " + args)
      }.value
    }
  }
  case class Arg(name: String, value: String)

  sealed abstract class Token
  case class StringToken(value: String) extends Token
  case class ArgToken(block: Block) extends Token

  sealed abstract class Value {
    def resolve(environment: Map[String, String]): String
  }
  case class SimpleString(string: String) extends Value {
    def resolve(environment: Map[String, String]): String = string
  }
  case class SubstitutionString(string: String) extends Value {
    def resolve(environment: Map[String, String]): String = string // TODO: BROKEN
  }

  class Parser extends JavaTokenParsers {
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

    def variableStatement = "set" ~> term ~ "=" ~ value ^^ { case name ~ _ ~ value =>
      VariableStatement(name, value)
    }

    def value = substitutionString | simpleString
    def substitutionString = "s" ~> stringLiteral ^^ { SubstitutionString(_) }
    def simpleString = stringLiteral ^^ { SimpleString(_) }

    def block: Parser[Block] = "{" ~> args <~ "}" ^^ { args =>
      Block(args)
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

    def parseLines(lines: TraversableOnce[String]): TraversableOnce[PipescriptParser.Statement] = {
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
