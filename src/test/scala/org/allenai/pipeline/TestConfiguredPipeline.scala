package org.allenai.pipeline

import java.io.File

import com.typesafe.config.{ ConfigValueFactory, ConfigValue, ConfigFactory }
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import spray.json.pimpAny

/** Created by rodneykinney on 5/12/15.
  */
class TestConfiguredPipeline extends UnitSpec with ScratchDirectory {
  val baseConfig = ConfigFactory.load()
    .withValue("output.persist.Step2", ConfigValueFactory.fromAnyRef("Step2Output.txt"))
    .withValue("output.persist.Step3", ConfigValueFactory.fromAnyRef("Step3Output.txt"))

  val step1 = new Producer[Int] with Ai2SimpleStepInfo {
    override def create = 1
  }
  case class AddOne(p: Producer[Int]) extends Producer[Int] with Ai2StepInfo {
    override def create = 1 + p.get
  }

  import IoHelpers._

  val format = SingletonIo.text[Int]

  it should "optionally persist" in {
    val outputDir = new File(scratchDir, "testOptionallyPersist")
    val pipeline = ConfiguredPipeline(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
    )
    pipeline.optionallyPersist(AddOne(step1), "Step2", format)

    pipeline.run("test")

    new File(outputDir, "data/Step2Output.txt") should exist
  }

  it should "recognize dryRun flag" in {
    val outputDir = new File(scratchDir, "testDryRun")
    val pipeline = ConfiguredPipeline(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
        .withValue("dryRun", ConfigValueFactory.fromAnyRef(true))
    )
    pipeline.optionallyPersist(AddOne(step1), "Step2", format)

    pipeline.run("test")

    new File(outputDir, "data/Step2Output.xt") should not(exist)
  }

  it should "recognize runOnly flag" in {
    val outputDir = new File(scratchDir, "testRunOnly")
    val config = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))

    // config specifies runOnly for step3 with no persisted upstream dependencies
    val pipeline = ConfiguredPipeline(config)
    val step2 = AddOne(step1)
    pipeline.optionallyPersist(AddOne(step2), "Step3", format)
    pipeline.run("test")

    new File(outputDir, "data/Step3Output.txt") should exist
  }

  it should "recognize runOnly flag and fail if upstream dependencies don't exist" in {
    val outputDir = new File(scratchDir, "testRunOnly2")
    val config = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))

    // config specifies runOnly for step3 but step2 is not persisted
    an[IllegalArgumentException] should be thrownBy {
      val pipeline = ConfiguredPipeline(config)
      val step2 = pipeline.optionallyPersist(AddOne(step1), "Step2", format)
      pipeline.optionallyPersist(AddOne(step2), "Step3", format)
      pipeline.run("test")
    }
  }

  it should "recognize tempOutputDir flag" in {
    val outputDir = new File(scratchDir, "testRunOnly3")
    val tempOutput = new File(outputDir, "temp-output")
    val config = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))
      .withValue("tempOutput", ConfigValueFactory.fromAnyRef(tempOutput.getCanonicalPath))

    // config specifies runOnly for step3 with no persisted upstream dependencies
    val pipeline = ConfiguredPipeline(config)
    val step2 = AddOne(step1)
    pipeline.optionallyPersist(AddOne(step2), "Step3", format)
    pipeline.run("test")

    new File(outputDir, "data/Step3Output.txt") should not(exist)
    new File(tempOutput, "data/Step3Output.txt") should exist
  }
}
