package org.allenai.pipeline.examples

import org.allenai.common.testkit.{UnitSpec, ScratchDirectory}

class TestCountWordsAndLines extends UnitSpec with ScratchDirectory {
  "pipeline" should "run" in {
    val outputs = CountWordsAndLinesPipeline.run(scratchDir).persistedSteps
    outputs.size should be > 0
    outputs.foreach{case (name, step) => assert(step.artifact.exists,s"Step $name not written")}
  }
}
