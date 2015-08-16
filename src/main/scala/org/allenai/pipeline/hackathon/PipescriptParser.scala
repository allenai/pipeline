package org.allenai.pipeline.hackathon

import org.apache.commons.lang3.StringEscapeUtils

import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import scala.util.parsing.combinator._

/** This class performs the basic parsing of the pipescript language.
  * It does not enforce semantic constraints on top of the syntax.
  * PipelineCompiler handles semantic constraints and builds something that is executable.
  */
object PipescriptParser {

  /** Each line in a pipescript file corresponds to a statement.
    */
  sealed abstract class Statement

  /** A comment statement is ignored.
    *
    * @param comment the text of the comment.
    */
  case class CommentStatement(comment: String) extends Statement

  /** A variable statement sets one or more variables to the associated values.
    */
  case class SetStatement(block: Block) extends Statement

  /** A package statement specifies the scripts to store so the experiment is repeatable.
    *
    * @param block a typesafe-config like block specifying arguments
    */
  case class PackageStatement(block: Block) extends Statement

  /** A step command describes one stage of a pipeline's execution.
    *
    * @param tokens a list of typesafe-config like block specifying arguments
    */
  case class StepStatement(tokens: Seq[Token]) extends Statement

  /** A block is JSON or Typesafe-Config like code that specifies a map.
    *
    * i.e. { name: "value", size: "5" }
    *
    * The only value supported is a string.
    *
    * @param args
    */
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

  /** A step command is composed of tokens.  They are either raw string tokens describing the
    * script to be run, or argument tokens describing input/output components from other parts
    * of the pipeline.
    */
  sealed abstract class Token
  case class StringToken(value: String) extends Token
  case class ArgToken(block: Block) extends Token

  /** A value is either a string, an s-string (for substitutions within a string), or a
    * variable reference (for convenience).
    */
  sealed abstract class Value {
    def resolve(environment: Map[String, String]): String
  }
  /** i.e. ${var} */
  case class VariableReference(name: String) extends Value {
    def resolve(environment: Map[String, String]): String =
      environment.get(name).getOrElse {
        throw new IllegalArgumentException(s"Could not find variable '$name' in environment: " +
          environment)
      }
  }

  /** A simple string is a Java string.
    * i.e. "Hel\"o"
    */
  case class SimpleString(stringLiteral: String) extends Value {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def resolve(environment: Map[String, String]): String = StringHelpers.unescape(stringBody)
  }
  object SimpleString {
    def from(s: String): SimpleString = SimpleString("\"" + s + "\"")
  }

  /** A scala-style substitution string."
    * s"Hello ${your.name}! My name is ${myName}"
    */
  case class SubstitutionString(stringLiteral: String) extends Value {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def resolve(environment: Map[String, String]): String = {
      case class Replacement(regex: Regex, logic: Match => String)

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
    val bracesVariableRegex = """(?<!\\)\$\{([^}]+)\}""".r
    val plainVariableRegex = """(?<!\\)\$(\w+)""".r
    def stripQuotes(s: String) = s.substring(1, s.length - 1)
    def unescape(s: String) = {
      StringEscapeUtils.unescapeJava(s)
    }
  }

  /** The parser combinator.  This is a subclass because parser combinators are not threadsafe. */
  class Parser extends JavaTokenParsers {
    def script: Parser[TraversableOnce[Statement]] = rep(line)

    def line = (comment | packageStatement | variableStatement | stepStatement)

    def comment = """#.*""".r ^^ { string => CommentStatement(string) }

    def packageStatement = "package" ~! block ^^ {
      case _ ~ b =>
        PackageStatement(b)
    }

    def stepStatement = "run" ~> rep(token) ^^ { case tokens => StepStatement(tokens) }

    def variableStatement = "set" ~! block ^^ {
      case _ ~ block =>
        SetStatement(block)
    }

    def token: Parser[Token] = argToken | stringToken

    def argToken = block ^^ { block => ArgToken(block) }

    def stringToken = nonKeyword | quotedKeyword

    def nonKeyword = not(reserved) ~> """[^{}\s`]+""".r ^^ StringToken

    def reserved = "run" | "package" | "set"

    def quotedKeyword = "`" ~> reserved <~ "`" ^^ StringToken

    def value = substitutionString | simpleString | variableReference
    def substitutionString = "s" ~> stringLiteral ^^ { s => SubstitutionString(s) }
    def simpleString = stringLiteral ^^ { s => SimpleString(s) }
    def variableReference = simpleVariableReference | complexVariableReference
    def simpleVariableReference = "$" ~> """\w+""".r ^^ { VariableReference(_) }
    def complexVariableReference = "${" ~> """\w+""".r <~ "}" ^^ { VariableReference(_) }

    /** A block is a JSON or Typesafe Config style block between braces.
      * However, it is never nested.
      *
      * { name: "pipescript", value: ${var} }
      */
    def block: Parser[Block] = "{" ~> args <~ "}" ^^ { args =>
      Block(args)
    }
    /** An argument list, separated by comma. */
    def args: Parser[List[Arg]] = repsep(arg, ",")
    /** An argument, which is a key, value pair. */
    def arg: Parser[Arg] = term ~ ":" ~ value ^^ { case term ~ ":" ~ value => Arg(term, value) }

    /** A term may only have letters. */
    def term: Parser[String] = """\w+""".r

    def parseScript(s: String) = {
      parseAll(script, s) match {
        case Success(statements, _) => statements
        case e: NoSuccess =>
          sys.error(s"Failed to parse script: $e")
      }
    }
  }
}
