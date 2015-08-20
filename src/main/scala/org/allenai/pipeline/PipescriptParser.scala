package org.allenai.pipeline

import org.apache.commons.lang3.StringEscapeUtils

import scala.collection.mutable.{ Map => MMap }
import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import scala.util.parsing.combinator.JavaTokenParsers

/** This class performs the basic parsing of the PipeScript language.
  * It does not enforce semantic constraints on top of the syntax.
  * PipelineCompiler handles semantic constraints and builds something that is executable.
  */
object PipeScriptParser {

  /** Each line in a pipescript file corresponds to a statement.
    */
  sealed abstract class Statement {
    /** Resolve variable references within this Statement.
      * A SetStatement will modify the environment
      * Other statements will use the environment to do variable substitution
      */
    def resolve(env: MMap[String, String]): Statement
  }

  /** A variable statement sets one or more variables to the associated values.
    */
  case class SetStatement(block: KeyValuePairs) extends Statement {
    def resolve(env: MMap[String, String]) = {
      val resolvedBlock = block.resolve(env)
      for (arg <- resolvedBlock.keyValuePairs) {
        env.put(arg.key, arg.value.asString)
      }
      SetStatement(resolvedBlock)
    }
  }

  /** A package statement specifies the scripts to store so the experiment is repeatable.
    *
    * @param block a typesafe-config like block specifying arguments
    */
  case class PackageStatement(block: KeyValuePairs) extends Statement {
    def resolve(env: MMap[String, String]) = PackageStatement(block.resolve(env))
  }

  /** A step command describes one stage of a pipeline's execution.
    *
    * @param tokens a list of typesafe-config like block specifying arguments
    */
  case class RunStatement(tokens: Seq[Token]) extends Statement {
    def resolve(env: MMap[String, String]) = RunStatement(tokens.map(_.resolve(env)))
  }

  /** A block is JSON or Typesafe-Config like code that specifies a map.
    *
    * i.e. { name: "value", size: "5" }
    *
    * The only value supported is a string.
    *
    * @param keyValuePairs
    */
  case class KeyValuePairs(keyValuePairs: Seq[KeyValue]) {
    def hasKey(name: String): Boolean = {
      keyValuePairs.exists(_.key == name)
    }

    def find(name: String): Option[String] = {
      keyValuePairs.find(_.key == name).map(_.value.asString)
    }

    def findGet(name: String): String = {
      find(name).getOrElse {
        throw new IllegalArgumentException(s"Could not find key '${name}' in arguments: " + keyValuePairs)
      }
    }

    def resolve(env: MMap[String, String]) = KeyValuePairs(keyValuePairs.map(_.resolve(env)))
  }

  case class KeyValue(key: String, value: StringExp) {
    def resolve(env: MMap[String, String]) = KeyValue(key, value.resolve(env))
  }

  /** A step command is composed of tokens.  They are either raw string tokens describing the
    * script to be run, or argument tokens describing input/output components from other parts
    * of the pipeline.
    */
  sealed abstract class Token {
    def resolve(env: MMap[String, String]): Token
  }

  case class StringToken(value: StringExp) extends Token {
    def resolve(env: MMap[String, String]) = StringToken(value.resolve(env))
  }

  case class KeyValuePairsToken(keyValuePairs: KeyValuePairs) extends Token {
    def resolve(env: MMap[String, String]) = KeyValuePairsToken(keyValuePairs.resolve(env))
  }

  /** A StringExp is either:
    * a string (quotes optional if it contains no special characters)
    * an s-string (for substitutions within a string),
    * or a variable reference.
    */
  sealed abstract class StringExp {
    def asString: String

    def resolve(environment: MMap[String, String]): LiteralString
  }

  /** i.e. ${var} */
  case class VariableReference(name: String) extends StringExp {
    def asString = sys.error(s"Unresolved variable $name")

    def resolve(environment: MMap[String, String]): LiteralString =
      environment.get(name).map(LiteralString.apply).getOrElse {
        throw new IllegalArgumentException(s"Could not find variable '$name' in environment: $environment")
      }
  }

  /** A string expressed in Java Syntax.
    * i.e. "Hel\"o"
    */
  case class JavaString(s: String) extends StringExp {
    def asString = StringHelpers.unescape(StringHelpers.stripQuotes(s))

    def resolve(environment: MMap[String, String]): LiteralString = LiteralString(asString)
  }

  /** A literal string without quotes, escaping, or anything else */
  case class LiteralString(s: String) extends StringExp {
    def asString = s

    def resolve(environment: MMap[String, String]): LiteralString = this
  }

  /** A scala-style substitution string."
    * s"Hello ${your.name}! My name is ${myName}"
    */
  case class SubstitutionString(stringLiteral: String) extends StringExp {
    val stringBody = StringHelpers.stripQuotes(stringLiteral)

    def asString: String = sys.error(s"String literal with unresolved escape characters: $stringLiteral")

    def resolve(env: MMap[String, String]): LiteralString = {
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

      LiteralString(StringHelpers.unescape(replaced))
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

    def line = packageStatement | variableStatement | stepStatement

    override protected val whiteSpace = """(\s|#.*)+""".r

    def packageStatement = "package" ~> propertyBag ^^ PackageStatement

    def stepStatement = "run" ~> rep(token) ^^ RunStatement

    def variableStatement = "set" ~> propertyBag ^^ SetStatement

    def token: Parser[Token] = keyValuePairsToken | stringToken

    def keyValuePairsToken = propertyBag ^^ KeyValuePairsToken

    def stringToken = (stringExp | nonKeywordOrQuotedKeyword) ^^ StringToken

    def nonKeywordOrQuotedKeyword = (nonKeyword | ("`" ~> reserved <~ "`")) ^^ LiteralString

    // We need to do backtracking in this one case, to handle unquoted strings with a keyword prefix
    def nonKeyword =
      new Parser[String] {
        def apply(in: Input) = {
          reserved(in) match {
            // Starts with a keyword. Fail if next token is whitespace
            case Success(_, next) =>
              if (next.atEnd || next.first == '#' || Character.isWhitespace(next.first)) {
                val matchedKeyword = in.source.subSequence(in.offset, next.offset)
                Failure(s"Keyword '$matchedKeyword' not allowed", next)
              } else {
                simpleString(in)
              }
            // Does not start with a keyword
            case Failure(_, _) => simpleString(in)
            case err => err
          }
        }
      }

    def simpleString = """[^{}\s`:,]+""".r

    def reserved = "run" | "package" | "set"

    def stringExp = substitutionString | javaString | variableReference

    def substitutionString = "s" ~> stringLiteral ^^ SubstitutionString

    def javaString = stringLiteral ^^ JavaString

    def variableReference = simpleVariableReference | complexVariableReference

    def simpleVariableReference = "$" ~> """\w+""".r ^^ VariableReference

    def complexVariableReference = "${" ~> """\w+""".r <~ "}" ^^ VariableReference

    /** A JSON or Typesafe Config style block between braces, without nesting
      *
      * { name: "pipescript", value: ${var} }
      */
    def propertyBag: Parser[KeyValuePairs] = "{" ~> keyValuePairs <~ "}" ^^ KeyValuePairs

    /** An argument list, separated by comma. */
    def keyValuePairs: Parser[List[KeyValue]] = repsep(keyValue, ",")

    /** An argument, which is a key, value pair. */
    def keyValue: Parser[KeyValue] = term ~ ":" ~ (stringExp | simpleString ^^ LiteralString) ^^ { case term ~ ":" ~ value => KeyValue(term, value) }

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
