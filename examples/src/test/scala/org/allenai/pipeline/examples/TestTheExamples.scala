package org.allenai.pipeline.examples

import org.allenai.common.testkit.{UnitSpec, ScratchDirectory}
import org.scalatest.FunSuite

class TestTheExamples extends FunSuite {
  test("CountWordsAndLinesPipeline should run") {
    try {
      org.allenai.pipeline.examples.CountWordsAndLinesPipeline.main(Array())
    } catch {
      case _:Throwable => assert(false)
    }
  }
  test("TrainModelPipeline should run") {
    try {
      org.allenai.pipeline.examples.TrainModelPipeline.main(Array())
    } catch {
      case _:Throwable => assert(false)
    }
  }
  test("TrainModelViaPythonPipeline should run") {
    try {
      org.allenai.pipeline.examples.TrainModelViaPythonPipeline.main(Array())
    } catch {
      case _:Throwable => assert(false)
    }
  }
}
