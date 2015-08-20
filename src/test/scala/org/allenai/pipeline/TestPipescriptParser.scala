package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec
import PipeScriptParser._

class TestPipescriptParser extends UnitSpec {
  "variable resolution" should "work with curly braces" in {
    val environment = collection.mutable.Map("x" -> "foo")
    val s = SubstitutionString("\"y = ${x}\"")
    assert(s.resolve(environment).asString === "y = foo")
  }

  it should "work without curly braces" in {
    val environment = collection.mutable.Map("x" -> "foo")
    val s = new SubstitutionString("\"y = $x\"")
    assert(s.resolve(environment).asString === "y = foo")
  }

  "pipeline scripting" should "successfully parse a step command" in {
    val program =
      """run python {in:"$scripts/ExtractArrows.py"} -i {in:"./png", id:"pngDir"} -o {out:"arrowDir", type:"dir"}"""
    val parser = new PipeScriptParser.Parser
    val parsed = parser.parseScript(program)
  }

  it should "parse a string without quotes" in {
    val program =
      """run python {in:./ExtractArrows.py} -i {in:png, id:pngDir}"""
    val parser = new PipeScriptParser.Parser
    val parsed = parser.parseScript(program)
  }

  it should "parse a set command" in {
    val program = """set {x: "foo"}"""
    val parser = new PipeScriptParser.Parser
    val parsed = parser.parseScript(program)
  }

  it should "parse a variable reference in a run command" in {
    val program =
      """set {x: foo.txt}
        |run echo $x
        |run echo s"abc-$x"
      """.stripMargin
    val script = PipeScriptCompiler.compileScript(program)
    assert(script.runCommands.map(_.scriptText).find(_.indexOf("echo foo.txt") >= 0).nonEmpty)
    assert(script.runCommands.map(_.scriptText).find(_.indexOf("echo abc-foo.txt") >= 0).nonEmpty)
  }

  it should "parse a small sample program" in {
    val simpleProgram =
      """| package {source: ./scripts, id: "scripts"}
        |
        |# Woohoo
        |run {input:"asdf",
        |     ignore:false} `run`
        |     {output:s"fdsa"}
        |
        |run echo done""".stripMargin

    val parser = new PipeScriptParser.Parser
    val parsed = parser.parseScript(simpleProgram).toList

    assert(parsed(0) === PackageStatement(KeyValuePairs(Seq(
      KeyValue("source", LiteralString("./scripts")),
      KeyValue("id", JavaString("\"scripts\""))
    ))))

    assert(parsed(1) === RunStatement(List(
      KeyValuePairsToken(KeyValuePairs(List(
        KeyValue("input", JavaString("\"asdf\"")),
        KeyValue("ignore", LiteralString("false"))
      ))),
      StringToken(LiteralString("run")),
      KeyValuePairsToken(KeyValuePairs(List(
        KeyValue("output", SubstitutionString("\"fdsa\""))
      )))
    )))

    assert(parsed(2) === RunStatement(List(
      StringToken(LiteralString("echo")), StringToken(LiteralString("done"))
    )))
  }
}
