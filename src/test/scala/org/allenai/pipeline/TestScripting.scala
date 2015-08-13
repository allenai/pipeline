package org.allenai.pipeline

import org.allenai.common.testkit.{ UnitSpec }

import scala.io.Source

import org.allenai.pipeline.Scripting._

class TestScripting extends UnitSpec {
  "pipeline scripting" should "successfully parse a step command" in {
    val program = """python {in:"$scripts/ExtractArrows.py"} -i {in:"./png", id:"pngDir"} -o {out:"arrowDir", type:"dir"}"""
    val parser = new Scripting.Parser
    val parsed = parser.parseAll(parser.stepStatement, program)
    assert(parser.parseAll(parser.stepStatement, program).successful)
  }

  "pipeline scripting" should "successfully parse a small sample program" in {
    val simpleProgram =
      """|package {source: "./scripts", id: "scripts"}
         |
         |# Woohoo
         |{in:"$scripts/asdf"} eek {out:"$scripts/asdf"}""".stripMargin

    val parser = new Scripting.Parser
    val parsed = parser.parse(simpleProgram)
    assert(parsed === Seq(
      PackageStatement(List(Arg("source", "./scripts"), Arg("id", "scripts"))),
      CommentStatement("# Woohoo"),
      StepStatement(List(ArgToken(Seq(Arg("in", "$scripts/asdf"))), StringToken("eek "), ArgToken(Seq(Arg("out", "$scripts/asdf")))))
    ))
  }

  "pipeline scripting" should "successfully parse the sample vision workflow" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionWorkflow = Source.fromURL(resourceUrl).getLines.toList

    val parser = new Scripting.Parser
    val parsed = parser.parseLines(visionWorkflow)

    assert(parsed.size > 0)
  }
}
