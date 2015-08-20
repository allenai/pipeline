package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec

import scala.io.Source

class TestPipescriptCompiler extends UnitSpec {
  "pipescript compiler" should "successfully parse and resolve a variable command" in {
    val program =
      """set {x: "http://www.foo.com"}
        |package {id: "pkg1", source: s"${x}"}
        |package {id: "pkg2", source: s"$x"}
      """.stripMargin
    val parsed = PipeScriptCompiler.compileScript(program)
  }

  it should "handle all sorts of variable substitution" in {
    def checkResult(scriptText: String, expectedArg: String) = {
      val script = PipeScriptCompiler.compileScript(scriptText)
      script.runCommands.head.tokens.head.scriptText should equal(expectedArg)
    }
    checkResult("""set {foo: bar} run $foo""", "bar")
    checkResult("""set {foo: bar} run ${foo}""", "bar")
    checkResult("""set {foo: bar} run s"$foo" """, "bar")
    checkResult("""set {foo: bar} run s"${foo}" """, "bar")
    checkResult("""set {bar: baz} set {foo: $bar} run $foo""", "baz")
    checkResult("""set {bar: baz} set {foo: ${bar}} run $foo""", "baz")
    checkResult("""set {bar: baz} set {foo: s"$bar"} run $foo""", "baz")
    checkResult("""set {bar: baz} set {foo: s"${bar}"} run $foo""", "baz")
    checkResult("""set {bar: baz} set {foo: s"${bar}ooka"} run $foo""", "bazooka")
  }

  it should "handle keywords appropriately" in {
    val compiler = PipeScriptCompiler
    def checkResult(scriptText: String, expectedArg: String) = {
      val script = compiler.compileScript(scriptText)
      script.runCommands.head.tokens.head.scriptText should equal(expectedArg)
    }

    checkResult("""set {run: bar} run $run""", "bar")
    checkResult("""set {run: package} run $run""", "package")
    checkResult("""set {running: setting} run $running""", "setting")
    checkResult("""run running""", "running")
    checkResult("""run `run`""", "run")

    compiler.compileScript("run run again").runCommands.map(_.tokens.size) should equal(List(0, 1))
    compiler.compileScript("run run").runCommands.map(_.tokens.size) should equal(List(0, 0))
    compiler.compileScript("run run#nothing\n run").runCommands.map(_.tokens.size) should equal(List(0, 0, 0))
  }

  it should "successfully parse the sample vision workflow" in {
    val scriptText = loadResource("/pipeline/vision-example/vision-workflow.pipe")

    val workflow = PipeScriptCompiler.compileScript(scriptText)

    assert(workflow.packages.size === 1)
    assert(workflow.runCommands.size === 4)
  }

  it should "handle comments correctly" in {
    val scriptText =
      """
        |# First line
        |run echo hi # End of line commend
        |# Line with keywords, such as set or package, or even run
        |run echo hi again
      """.stripMargin
    val script = PipeScriptCompiler.compileScript(scriptText)
    script.runCommands.size should equal(2)
    // Make sure we don't include the comments as tokens in the run statement
    script.runCommands.map(_.tokens.size).sorted.toList should equal(List(2, 3))
  }

  it should "successfully parse the sample aristo workflow" in {
    val scriptText = loadResource("/pipeline/aristo-example/ablation-study.pipe")

    val workflow = PipeScriptCompiler.compileScript(scriptText)

    assert(workflow.packages.size === 3)
    assert(workflow.runCommands.size === 7)
  }

  it should "build a pipeline from a script" in {
    val scriptText = loadResource("/pipeline/vision-example/vision-workflow.pipe")

    val parser = PipeScriptCompiler
    val script = parser.compileScript(scriptText)
  }

  it should "run a pipeline from a script" in {
    val scriptText = loadResource("/pipeline/vision-example/vision-workflow.pipe")

    val dir = new File(new File("pipeline-output"), "RunScript")
    val pipeline = new PipeScriptInterpreter(Pipeline(dir)).buildPipeline(scriptText)
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
