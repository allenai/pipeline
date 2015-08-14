package org.allenai.pipeline.hackathon

import java.io.File

import org.allenai.common.testkit.UnitSpec
import org.allenai.pipeline.hackathon.PipelineScript._

import scala.io.Source

class TestPipelineScript extends UnitSpec {
  "pipeline scripting" should "successfully parse a step command" in {
    val program = """python {in:"$scripts/ExtractArrows.py"} -i {in:"./png", id:"pngDir"} -o {out:"arrowDir", type:"dir"}"""
    val parser = new PipelineScript.Parser
    val parsed = parser.parseAll(parser.stepStatement, program)
    assert(parsed.successful)
  }

  it should "successfully parse a variable command" in {
    val program = """set x = foo"""
    val parser = new PipelineScript.Parser
    val parsed = parser.parseAll(parser.variableStatement, program)
    assert(parsed.successful)
  }

  it should "successfully parse and resolve a variable command" in {
    val program =
      """set x = foo
        |package {id: "pkg", source: ${x}}
      """.stripMargin
    val parser = new PipelineScriptParser
    val parsed = parser.parseText(null)(program)
  }

  /*
  it should "successfully parse and use a variable command" in {
    val program =
      """set x = foo
        |echo {in: "$x"}
      """.stripMargin
    val parser = new PipelineScript.Parser
    val parsed = parser.parseText(program).toSeq
    assert(parsed.length === 2)
    assert(parsed(1).isInstanceOf[StepStatement])
    assert(parsed(1).asInstanceOf[StepStatement].tokens(1).asInstanceOf[ArgToken].args.find(_
        .name == "in").get.value === "foo")
  }
  */

  ignore should "successfully parse a small sample program" in {
    val simpleProgram =
      """|package {source: "./scripts", id: "scripts"}
        |
        |# Woohoo
        |{in:"$scripts/asdf"} eek {out:"$scripts/asdf"}""".stripMargin

    val parser = new PipelineScript.Parser
    val parsed = parser.parseText(simpleProgram).toSeq
    assert(parsed === Seq(
      PackageStatement(Block("""{source: "./scripts", id: "scripts"}""")),
      CommentStatement("# Woohoo"),
      StepStatement(Seq(
        ArgToken(Block("""{in:"$scripts/asdf"}""")),
        StringToken("eek "),
        ArgToken(Block("""{out:"$scripts/asdf"}""")))))
    )
  }

  it should "successfully parse the sample vision workflow" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionWorkflow = Source.fromURL(resourceUrl).getLines.toList

    val parser = new PipelineScriptParser
    val parsed = parser.parseLines(null)(visionWorkflow)
  }

  it should "build a pipeline from a script" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionWorkflow = Source.fromURL(resourceUrl).getLines.toList

    val parser = new PipelineScriptParser()
    val script = parser.parseLines(null)(visionWorkflow)
  }

  it should "run a pipeline from a script" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionScriptLines = Source.fromURL(resourceUrl).getLines.toList

    val dir = new File(new File("pipeline-output"), "RunScript").toURI
    //    val dir = new java.net.URI("s3://ai2-misc/hackathon-2015/pipeline/")
    val pipeline = WorkflowScriptPipeline.buildPipeline(dir, visionScriptLines)
    val scriptlink = new ReplicateFile(new java.io.File(resourceUrl.toURI), None, dir, pipeline.artifactFactory)
    scriptlink.get
    pipeline.run("RunFromScript", Some(pipeline.toHttpUrl(scriptlink.artifact.url)))
  }
}
