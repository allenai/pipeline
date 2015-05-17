package org.allenai.pipeline

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers._

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }

import java.io.File

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

  val format = SingletonIo.text[Int]

  it should "optionally persist" in {
    val outputDir = new File(scratchDir, "testOptionallyPersist")
    val pipeline = new ConfiguredPipeline(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
    )
    val step = pipeline.optionallyPersist(AddOne(step1), "Step2", format)
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should exist
  }

  it should "optionally persist to absolute URL" in {
    val outputDir = new File(scratchDir, "testOptionallyPersist2")
    val configuredFile = new File(scratchDir, "subDir/mySpecialPath")
    val pipeline = new ConfiguredPipeline(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
        .withValue("output.persist.Step2", ConfigValueFactory.fromAnyRef(configuredFile.toURI.toString))
    )
    val step = pipeline.optionallyPersist(AddOne(step1), "Step2", format)
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should exist
    outputFile.getCanonicalFile should equal(configuredFile.getCanonicalFile)
  }

  it should "recognize dryRun flag" in {
    val outputDir = new File(scratchDir, "testDryRun")
    val pipeline = new ConfiguredPipeline(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
        .withValue("dryRun", ConfigValueFactory.fromAnyRef(true))
    )
    val step = pipeline.optionallyPersist(AddOne(step1), "Step2", format)
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should not(exist)
  }

  it should "recognize runOnly flag" in {
    val outputDir = new File(scratchDir, "testRunOnly")
    val config = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))

    // config specifies runOnly for step3 with no persisted upstream dependencies
    val pipeline = new ConfiguredPipeline(config)
    val step2 = AddOne(step1)
    val step2Persisted = pipeline.optionallyPersist(AddOne(step2), "Step3", format)
    val outputFile = new File(step2Persisted.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)
    pipeline.run("test")

    outputFile should exist
  }

  it should "recognize runOnly flag and fail if upstream dependencies don't exist" in {
    val outputDir = new File(scratchDir, "testRunOnly2")
    val config = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))

    // config specifies runOnly for step3 but step2 is not persisted
    an[IllegalArgumentException] should be thrownBy {
      val pipeline = new ConfiguredPipeline(config)
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
      .withValue("tmpOutput", ConfigValueFactory.fromAnyRef(tempOutput.getCanonicalPath))

    // config specifies runOnly for step3 with no persisted upstream dependencies
    val pipeline = new ConfiguredPipeline(config)
    val step2 = AddOne(step1)
    val step2Persisted = pipeline.optionallyPersist(AddOne(step2), "Step3", format)
    pipeline.run("test")

    val outputFile = new File(step2Persisted.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)
    outputFile.getParentFile.getParentFile.getCanonicalFile should equal(tempOutput.getCanonicalFile)
  }
}
