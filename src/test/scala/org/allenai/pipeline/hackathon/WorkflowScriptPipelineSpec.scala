package org.allenai.pipeline.hackathon

import java.net.URI
import org.allenai.common.testkit.UnitSpec

class WorkflowScriptPipelineSpec extends UnitSpec {

  "WorkflowScriptPipeline" should "work" in {
    val script = WorkflowScript(
      packages = Nil,
      stepCommands = Nil,
      outputDir = new URI("s3://ai2-s2-dev/pipeline-hackathon/")
    )

    val pipeline = new WorkflowScriptPipeline(script).buildPipeline

  }
}
