package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

class TestPipeline extends UnitSpec with ScratchDirectory {
  case class AddOne(input: Producer[Int]) extends Producer[Int] with Ai2StepInfo {
    override def create = input.get + 1
  }
  "Pipeline" should "run all persisted targets" in {
    val p1 = Producer.fromMemory(1)
    val p2 = Producer.fromMemory(2)
    val p3 = Producer.fromMemory(3)
    val p4 = Producer.fromMemory(4)
    val outputDir = new File(scratchDir, "test1")
    val pipeline = Pipeline.saveToFileSystem(outputDir)

    import IoHelpers._
    pipeline.Persist.Singleton.asText(p1, Some("prod1"))
    pipeline.Persist.Singleton.asText(p2, Some("prod2"))
    pipeline.Persist.Singleton.asText(p3, Some("prod3"))
    val p4Persisted = pipeline.Persist.Singleton.asText(p4, Some("prod4"))

    an[IllegalArgumentException] should be thrownBy {
      val p5 = AddOne(p4Persisted)
      pipeline.Persist.Singleton.asText(p5, Some("prod5"))
      // p5 has p4 as a dependency, but p4 has not been computed yet
      pipeline.runOnly("test", p5)
    }

    pipeline.runOnly("test", p1)
    new File(outputDir, "data/prod1") should exist
    new File(outputDir, "data/prod2") should not(exist)

    pipeline.run("test")
    new File(outputDir, "data/prod2") should exist
    new File(outputDir, "data/prod3") should exist
    new File(outputDir, "data/prod4") should exist

    an[IllegalArgumentException] should be thrownBy {
      val p5 = Producer.fromMemory(5)
      // p5 has not been persisted, so we should not specify it as an output
      pipeline.runOnly("test", p5)
    }

    val p5 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 5
      override def stepInfo = super.stepInfo.addParameters(("upstream", p4))
    }
    pipeline.Persist.Singleton.asText(p5, Some("prod5"))
    // p5 is persisted and its dependency exists.  All clear!
    pipeline.runOnly("test", p5)
  }

  it should "respect both relative and absolute persistence paths" in {
    val p1 = Producer.fromMemory(1)
    val p2 = Producer.fromMemory(2)

    val outputDir = new File(scratchDir, "test2")
    val pipeline = Pipeline.saveToFileSystem(outputDir)

    import IoHelpers._
    pipeline.Persist.Singleton.asText(p1, Some("prod1"))
    pipeline.Persist.Singleton.asText(p2, Some(s"file://$scratchDir/absolute-path/prod2"))

    pipeline.run("test")
    new File(scratchDir, "absolute-path/prod2") should exist
  }

  it should "respect tmpOutput argument in runOne()" in {
    val p1 = Producer.fromMemory(1)

    val outputDir = new File(scratchDir, "testRunOne")
    val pipeline = Pipeline.saveToFileSystem(outputDir)

    import IoHelpers._
    val p2 = pipeline.Persist.Singleton.asText(AddOne(p1), Some("prod2"))

    val tmpOutput = new File(outputDir, "temp-output/prod2")
    pipeline.runOne(p2, Some(new FileArtifact(tmpOutput)))

    new File(outputDir, "data/prod2") should not(exist)
    tmpOutput should exist
  }

}
