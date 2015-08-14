package org.allenai.pipeline.hackathon

import org.apache.commons.lang3.StringEscapeUtils

import scala.util.matching.Regex
import scala.util.matching.Regex.Match
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

    def find(name: String): Option[Value] = {
      args.find(_.name == name).map(_.value)
    }

    def findGet(name: String): Value = {
      args.find(_.name == name).getOrElse {
        throw new IllegalArgumentException(s"Could not find key '${name}' in arguments: " + args)
      }.value
    }
  }
  case class Arg(name: String, value: Value)

  sealed abstract class Token
  case class StringToken(value: String) extends Token
  case class ArgToken(block: Block) extends Token

  sealed abstract class Value {
    def resolve(environment: Map[String, String]): String
  }
  case class VariableReference(name: String) extends Value {
    def resolve(environment: Map[String, String]): String =
      environment.get(name).getOrElse {
        throw new IllegalArgumentException(s"Could not find variable '$name' in environment: " +
            environment)
      }
  }
  case class SimpleString(stringLiteral: String) extends Value {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def resolve(environment: Map[String, String]): String = StringHelpers.unescape(stringBody)
  }
  case class SubstitutionString(stringLiteral: String) extends Value {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def resolve(environment: Map[String, String]): String = {
      case class Replacement(regex: Regex, logic: Match=>String)

      def lookup(m: Match): String = {
        val ref = m.group(1)
        val value = environment.get(ref).getOrElse {
          throw new IllegalArgumentException(s"Could not find variable '$ref' in environment: " +
              environment)
        }

        value
      }

      val replacements = Seq(
        Replacement(StringHelpers.bracesVariableRegex, lookup),
        Replacement(StringHelpers.plainVariableRegex, lookup)
      )

      val replaced = replacements.foldLeft(stringBody) { (string, replacement) =>
        replacement.regex.replaceAllIn(string, replacement.logic)
      }

      StringHelpers.unescape(replaced)
    }
  }
  object StringHelpers {
    val bracesVariableRegex = """\$\{([^}]+)\}""".r
    val plainVariableRegex = """\$(\w+)""".r
    def stripQuotes(s: String) = s.substring(1, s.length - 1)
    def unescape(s: String) = {
      StringEscapeUtils.unescapeJava(s)
    }
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

    def value = substitutionString | simpleString | variableReference
    def substitutionString = "s" ~> stringLiteral ^^ { s => SubstitutionString(s) }
    def simpleString = stringLiteral ^^ { s => SimpleString(s) }
    def variableReference = simpleVariableReference | complexVariableReference
    def simpleVariableReference = "$" ~> """\w+""".r  ^^ { VariableReference(_) }
    def complexVariableReference = "${" ~> """\w+""".r <~ "}" ^^ { VariableReference(_) }

    def block: Parser[Block] = "{" ~> args <~ "}" ^^ { args =>
      Block(args)
    }
    def args: Parser[List[Arg]] = repsep(arg, ",")
    def arg: Parser[Arg] = term ~ ":" ~ value ^^ { case term ~ ":" ~ value => Arg(term, value) }

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
