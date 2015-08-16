package org.allenai.pipeline.hackathon

import org.apache.commons.lang3.StringEscapeUtils
import scala.collection.mutable.{ Map => MMap }

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
  sealed abstract class Statement {
    /** Resolve variable references within this Statement.
      * A SetStatement will modify the environment
      * Other statements will use the environment to do variable substitution
      */
    def resolve(env: MMap[String, String]): Statement
  }

  /** A comment statement is ignored.
    *
    * @param comment the text of the comment.
    */
  case class CommentStatement(comment: String) extends Statement {
    def resolve(env: MMap[String, String]) = this
  }

  /** A variable statement sets one or more variables to the associated values.
    */
  case class SetStatement(block: Block) extends Statement {
    def resolve(env: MMap[String, String]) = {
      val resolvedBlock = block.resolve(env)
      for (arg <- resolvedBlock.args) {
        env.put(arg.name, arg.value.asInstanceOf[SimpleString].stringLiteral)
      }
      SetStatement(resolvedBlock)
    }
  }
  /** A package statement specifies the scripts to store so the experiment is repeatable.
    *
    * @param block a typesafe-config like block specifying arguments
    */
  case class PackageStatement(block: Block) extends Statement {
    def resolve(env: MMap[String, String]) = PackageStatement(block.resolve(env))
  }

  /** A step command describes one stage of a pipeline's execution.
    *
    * @param tokens a list of typesafe-config like block specifying arguments
    */
  case class StepStatement(tokens: Seq[Token]) extends Statement {
    def resolve(env: MMap[String, String]) = StepStatement(tokens.map(_.resolve(env)))
  }

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

    def find(name: String): Option[String] = {
      args.find(_.name == name).map(_.value.asString)
    }

    def findGet(name: String): String = {
      find(name).getOrElse {
        throw new IllegalArgumentException(s"Could not find key '${name}' in arguments: " + args)
      }
    }
    def resolve(env: MMap[String, String]) = Block(args.map(_.resolve(env)))
  }
  case class Arg(name: String, value: Value) {
    def resolve(env: MMap[String, String]) = Arg(name, value.resolve(env))
  }

  /** A step command is composed of tokens.  They are either raw string tokens describing the
    * script to be run, or argument tokens describing input/output components from other parts
    * of the pipeline.
    */
  sealed abstract class Token {
    def resolve(env: MMap[String, String]): Token
  }
  case class StringToken(value: String) extends Token {
    def resolve(env: MMap[String, String]) = this
  }
  case class ArgToken(block: Block) extends Token {
    def resolve(env: MMap[String, String]) = ArgToken(block.resolve(env))
  }

  /** A value is either a string, an s-string (for substitutions within a string), or a
    * variable reference (for convenience).
    */
  sealed abstract class Value {
    def asString: String
    def resolve(environment: MMap[String, String]): SimpleString
  }
  /** i.e. ${var} */
  case class VariableReference(name: String) extends Value {
    def asString = sys.error(s"Unresolved variable $name")
    def resolve(environment: MMap[String, String]): SimpleString =
      environment.get(name).map(SimpleString.apply).getOrElse {
        throw new IllegalArgumentException(s"Could not find variable '$name' in environment: $environment")
      }
  }

  /** A simple string is a Java string.
    * i.e. "Hel\"o"
    */
  case class SimpleString(stringLiteral: String) extends Value {
    def asString = stringLiteral
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def resolve(environment: MMap[String, String]): SimpleString = SimpleString(StringHelpers.unescape(stringBody))
  }
  object SimpleString {
    def from(s: String): SimpleString = SimpleString("\"" + s + "\"")
  }

  /** A scala-style substitution string."
    * s"Hello ${your.name}! My name is ${myName}"
    */
  case class SubstitutionString(stringLiteral: String) extends Value {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)
    def asString: String = sys.error(s"String literal with unresolved escape characters: $stringLiteral")
    def resolve(env: MMap[String, String]): SimpleString = {
      case class Replacement(regex: Regex, logic: Match => String)

      def lookup(m: Match): String = {
        val ref = m.group(1)
        val value = env.get(ref).getOrElse {
          throw new IllegalArgumentException(s"Could not find variable '$ref' in environment: " +
            env)
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

      SimpleString(StringHelpers.unescape(replaced))
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

    def line = comment | packageStatement | variableStatement | stepStatement

    def comment = """#.*""".r ^^ CommentStatement

    def packageStatement = "package" ~> block ^^ PackageStatement

    def stepStatement = "run" ~> rep(token) ^^ StepStatement

    def variableStatement = "set" ~> block ^^ SetStatement

    def token: Parser[Token] = argToken | stringToken

    def argToken = block ^^ { block => ArgToken(block) }

    def stringToken = nonKeyword | quotedKeyword

    def nonKeyword = not(reserved) ~> """[^{}\s`]+""".r ^^ StringToken

    def reserved = "run" | "package" | "set"

    def quotedKeyword = "`" ~> reserved <~ "`" ^^ StringToken

    def value = substitutionString | simpleString | variableReference
    def substitutionString = "s" ~> stringLiteral ^^ SubstitutionString
    def simpleString = stringLiteral ^^ { s => SimpleString(s) }
    def variableReference = simpleVariableReference | complexVariableReference
    def simpleVariableReference = "$" ~> """\w+""".r ^^ VariableReference
    def complexVariableReference = "${" ~> """\w+""".r <~ "}" ^^ VariableReference

    /** A block is a JSON or Typesafe Config style block between braces.
      * However, it is never nested.
      *
      * { name: "pipescript", value: ${var} }
      */
    def block: Parser[Block] = "{" ~> args <~ "}" ^^ Block

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
