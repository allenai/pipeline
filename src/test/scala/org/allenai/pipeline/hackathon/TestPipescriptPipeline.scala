package org.allenai.pipeline.hackathon

import org.allenai.common.Logging
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

class TestPipescriptPipeline extends UnitSpec with ScratchDirectory {
  "PipescriptPipeline" should "work" in {
    val pipeline = PipescriptPipeline.buildPipeline(TestData.script)
    pipeline.run("WOOOT!")
  }
}
