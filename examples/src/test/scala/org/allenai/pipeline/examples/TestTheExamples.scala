package org.allenai.pipeline.examples

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

class TestTheExamples extends UnitSpec with ScratchDirectory {
  "CountWordsAndLinesPipeline" should "run" in {
    CountWordsAndLinesPipeline.main(Array())
  }
  "TrainModelPipeline" should "run" in {
    TrainModelPipeline.main(Array())
  }
  "TrainModelViaPythonPipeline" should "run" in {
    TrainModelViaPythonPipeline.main(Array())
  }
}
