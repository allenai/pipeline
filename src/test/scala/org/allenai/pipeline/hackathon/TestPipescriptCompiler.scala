package org.allenai.pipeline.hackathon

import org.allenai.common.testkit.UnitSpec

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
    val parsed = parser.parseText(null)(program)
  }

  it should "successfully parse the sample vision workflow" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionWorkflow = Source.fromURL(resourceUrl).getLines.toList

    val parser = new PipescriptCompiler
    val workflow = parser.parseLines(null)(visionWorkflow)

    assert(workflow.packages.size === 1)
    assert(workflow.stepCommands.size === 4)
  }

  it should "build a pipeline from a script" in {
    val resourceUrl = {
      val url = this.getClass.getResource("/pipeline/vision-workflow.pipe")
      require(url != null, "Could not find resource.")
      url
    }
    val visionWorkflow = Source.fromURL(resourceUrl).getLines.toList

    val parser = new PipescriptCompiler()
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
    pipeline.run("RunFromScript", None)
  }
}
