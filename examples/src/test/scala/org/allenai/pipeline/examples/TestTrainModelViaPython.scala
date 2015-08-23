package org.allenai.pipeline.examples

import org.allenai.common.testkit.{ScratchDirectory, UnitSpec}

class TestTrainModelViaPython extends UnitSpec with ScratchDirectory {
   "pipeline" should "run" in {
     val outputs = TrainModelViaPythonPipeline.run(scratchDir).persistedSteps.values
     outputs.size should be > 0
     outputs.foreach(s => assert(s.artifact.exists,s"Step $s not written"))
   }
 }
