package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

class TestPipeline extends UnitSpec with ScratchDirectory {
  "Pipeline" should "run all persisted targets" in {
    val p1 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 1
    }
    val p2 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 2
    }
    val p3 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 3
    }
    val p4 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 4
    }
    val outputDir = new File(scratchDir, "test1")
    val pipeline = new Pipeline {
      val artifactFactory = new RelativeFileSystem(outputDir)
    }

    import IoHelpers._
    val format = SingletonIo.text[Int]
    pipeline.persist(p1, format, Some("prod1"))
    pipeline.persist(p2, format, Some("prod2"))
    pipeline.persist(p3, format, Some("prod3"))
    val p4Persisted = pipeline.persist(p4, format, Some("prod4"))

    an[IllegalArgumentException] should be thrownBy {
      val p5 = new Producer[Int] with Ai2SimpleStepInfo {
        override def create = 5
        override def stepInfo = super.stepInfo.addParameters(("upstream", p4Persisted))
      }
      pipeline.persist(p5, format, Some("prod5"))
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
      val p5 = new Producer[Int] with Ai2SimpleStepInfo {
        override def create = 5
      }
      // p5 has not been persisted, so we should not specify it as an output
      pipeline.runOnly("test", p5)
    }

    val p5 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 5
      override def stepInfo = super.stepInfo.addParameters(("upstream", p4))
    }
    pipeline.persist(p5, format, Some("prod5"))
    // p5 is persisted and its dependency exists.  All clear!
    pipeline.runOnly("test", p5)
  }

  "Pipeline" should "respect both relative and absolute persistence paths" in {
    val p1 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 1
    }
    val p2 = new Producer[Int] with Ai2SimpleStepInfo {
      override def create = 2
    }

    val outputDir = new File(scratchDir, "test2")
    val pipeline = new Pipeline {
      val artifactFactory = new RelativeFileSystem(outputDir)
    }

    import IoHelpers._
    val format = SingletonIo.text[Int]
    pipeline.persist(p1, format, Some("prod1"))
    pipeline.persist(p2, format, Some(s"file://$scratchDir/absolute-path/prod2"))

    pipeline.run("test")
    new File(scratchDir, "absolute-path/prod2") should exist
  }

}
