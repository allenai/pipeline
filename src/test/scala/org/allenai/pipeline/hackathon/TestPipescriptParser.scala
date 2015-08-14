package org.allenai.pipeline.hackathon

import org.allenai.common.testkit.UnitSpec
import org.allenai.pipeline.hackathon.PipescriptParser._

class TestPipescriptParser extends UnitSpec {
  "variable resolution" should "work with curly braces" in {
    val environment = Map("x" -> "foo")
    val s = SubstitutionString("y = ${x}")
    assert(s.resolve(environment) === "y = foo")
  }

  it should "work without curly braces" in {
    val environment = Map("x" -> "foo")
    val s = new SubstitutionString("y = $x")
    assert(s.resolve(environment) === "y = foo")
  }

  "pipeline scripting" should "successfully parse a step command" in {
    val program = """python {in:"$scripts/ExtractArrows.py"} -i {in:"./png", id:"pngDir"} -o {out:"arrowDir", type:"dir"}"""
    val parser = new PipescriptParser.Parser
    val parsed = parser.parseAll(parser.stepStatement, program)
    assert(parsed.successful)
  }

  it should "successfully parse a variable command" in {
    val program = """set x = "foo""""
    val parser = new PipescriptParser.Parser
    val parsed = parser.parseAll(parser.variableStatement, program)
    assert(parsed.successful)
  }

  /*
  it should "successfully parse and use a variable command" in {
    val program =
      """set x = foo
        |echo {in: "$x"}
      """.stripMargin
    val parser = new PipescriptParser.Parser
    val parsed = parser.parseText(program).toSeq
    assert(parsed.length === 2)
    assert(parsed(1).isInstanceOf[StepStatement])
    assert(parsed(1).asInstanceOf[StepStatement].tokens(1).asInstanceOf[ArgToken].args.find(_
        .name == "in").get.value === "foo")
  }
  */

  it should "successfully parse a small sample program" in {
    val simpleProgram =
      """|package {source: "./scripts", id: "scripts"}
        |
        |# Woohoo
        |{in:"asdf"} eek {out:"fdsa"}""".stripMargin

    val parser = new PipescriptParser.Parser
    val parsed = parser.parseText(simpleProgram).toList

    assert(parsed(0) === PackageStatement(Block(Seq(
      Arg("source", SimpleString("./scripts")),
      Arg("id", SimpleString("scripts"))
    ))))

    assert(parsed(1).isInstanceOf[CommentStatement])

    assert(parsed(2) === StepStatement(List(
      ArgToken(Block(List(Arg("in", SimpleString("asdf"))))),
      StringToken("eek"),
      ArgToken(Block(List(Arg("out", SimpleString("fdsa"))))))
    ))
  }
}
