package org.allenai.pipeline

import java.io.File

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers._

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
    val outputDir = new File(scratchDir, "testpersist")
    val pipeline = Pipeline.configured(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
    )
    val step = pipeline.persist(AddOne(step1), format)
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should exist
  }

  it should "optionally persist to absolute URL" in {
    val outputDir = new File(scratchDir, "testpersist2")
    val configuredFile = new File(scratchDir, "subDir/mySpecialPath")
    val pipeline = Pipeline.configured(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
        .withValue("output.persist.Step2", ConfigValueFactory.fromAnyRef(configuredFile.toURI.toString))
    )
    val step = pipeline.persist(AddOne(step1), format, "Step2")
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should exist
    outputFile.getCanonicalFile should equal(configuredFile.getCanonicalFile)
  }

  it should "recognize dryRun flag" in {
    val outputDir = new File(scratchDir, "testDryRun")
    val pipeline = Pipeline.configured(
      baseConfig
        .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
        .withValue("dryRun", ConfigValueFactory.fromAnyRef(true))
        .withValue("dryRunOutput", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
    )
    val step = pipeline.persist(AddOne(step1), format, "Step2")
    val outputFile = new File(step.asInstanceOf[PersistedProducer[Int, FlatArtifact]].artifact.url)

    pipeline.run("test")

    outputFile should not(exist)
  }

  it should "recognize runOnly flag" in {
    val outputDir = new File(scratchDir, "testRunOnly")
    val cfg = baseConfig
      .withValue("output.dir", ConfigValueFactory.fromAnyRef(outputDir.getCanonicalPath))
      .withValue("runOnly", ConfigValueFactory.fromAnyRef("Step3"))

    // config specifies runOnly for step3 with no persisted upstream dependencies
    val pipeline = Pipeline.configured(cfg)
    val step2 = AddOne(step1)
    val step2Persisted = pipeline.persist(AddOne(step2), format, "Step3")
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
      val pipeline = Pipeline.configured(config)
      val step2 = pipeline.persist(AddOne(step1), format, "Step2")
      pipeline.persist(AddOne(step2), format, "Step3")
      pipeline.run("test")
    }
  }
}
