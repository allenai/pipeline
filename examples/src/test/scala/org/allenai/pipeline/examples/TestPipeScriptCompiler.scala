package org.allenai.pipeline.examples

import org.allenai.common.testkit.UnitSpec
import org.allenai.pipeline.PipeScriptCompiler

import scala.io.Source

class TestPipeScriptCompiler extends UnitSpec {
  "PipeScriptCompiler" should "parse the aristo example" in {
    val scriptText = loadResource("/pipeline/aristo-example/ablation-study.pipe")

    val workflow = PipeScriptCompiler.compileScript(scriptText)

    assert(workflow.packages.size === 3)
    assert(workflow.runCommands.size === 7)
  }

  it should "parse the vision example" in {
    val scriptText = loadResource("/pipeline/vision-example/vision-workflow.pipe")

    val parser = PipeScriptCompiler
    val script = parser.compileScript(scriptText)
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
