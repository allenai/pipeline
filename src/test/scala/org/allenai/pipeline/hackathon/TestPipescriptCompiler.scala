package org.allenai.pipeline.hackathon

import org.allenai.common.testkit.UnitSpec
import org.allenai.pipeline.Pipeline

import scala.io.Source

import java.io.File

class TestPipescriptCompiler extends UnitSpec {
  "pipescript compiler" should "successfully parse and resolve a variable command" in {
    val program =
      """set {x: "http://www.foo.com"}
        |package {id: "pkg1", source: s"${x}"}
        |package {id: "pkg2", source: s"$x"}
      """.stripMargin
    val parser = new PipescriptCompiler
    val parsed = parser.compileScript(program)
  }

  it should "successfully parse the sample vision workflow" in {
    val scriptText = loadResource("/pipeline/vision-workflow.pipe")

    val parser = new PipescriptCompiler
    val workflow = parser.compileScript(scriptText)

    assert(workflow.packages.size === 1)
    assert(workflow.stepCommands.size === 4)
  }

  it should "handle comments correctly" in {
//    val p = new PipescriptParser.Parser()
//    assert(p.parse(p.comment, "#hi, there\n#run echo hi").successful)
    val scriptText =
      """
        |# First line
        |run echo hi # End of line commend
        |# Line with keywords, such as set
        |run echo hi again
      """.stripMargin
    val script = new PipescriptCompiler().compileScript(scriptText)
    script.stepCommands.size should equal(2)
    // Make sure we don't include the comments as tokens in the run statement
    script.stepCommands.map(_.tokens.size).sorted.toList should equal(List(2,3))
  }

  it should "successfully parse the sample aristo workflow" in {
    val scriptText = loadResource("/pipeline/ablation-study.pipe")

    val parser = new PipescriptCompiler
    val workflow = parser.compileScript(scriptText)

    assert(workflow.packages.size === 3)
//    assert(workflow.stepCommands.size > 0)
    assert(workflow.stepCommands.size === 7)
  }

  it should "build a pipeline from a script" in {
    val scriptText = loadResource("/pipeline/vision-workflow.pipe")

    val parser = new PipescriptCompiler()
    val script = parser.compileScript(scriptText)
  }

  it should "run a pipeline from a script" in {
    val scriptText = loadResource("/pipeline/vision-workflow.pipe")

    val dir = new File(new File("pipeline-output"), "RunScript")
    val pipeline = new PipescriptPipeline(Pipeline(dir)).buildPipeline(scriptText)
    pipeline.run("RunFromScript", None)
  }

  def loadResource(path: String) = {
    val resourceUrl = {
      val url = this.getClass.getResource(path)
      require(url != null, s"Could not find resource $path")
      url
    }
    Source.fromURL(resourceUrl).mkString
  }
}
