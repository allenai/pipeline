package org.allenai.pipeline.hackathon

import org.allenai.common.Logging
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

class WorkflowScriptPipelineSpec extends UnitSpec with ScratchDirectory {
  "WorkflowScriptPipeline" should "work" in {
    val pipeline = WorkflowScriptPipeline.buildPipeline(TestData.script)
    pipeline.run("WOOOT!")
  }
}

object WorflowScriptTester extends App with Logging {
  val pipeline = WorkflowScriptPipeline.buildPipeline(TestData.script)
  pipeline.run("WOOOT!")
  // pipeline.dryRun(
  //   outputDir = new File("/Users/markschaake/tmp"),
  //   rawTitle = "Woohoo!"
  // )
}
