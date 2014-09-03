package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
class TestProducer extends UnitSpec with BeforeAndAfterAll {
  import scala.language.reflectiveCalls
  val rand = new Random
  import org.allenai.pipeline.IOHelpers._

  val outputDir = new File("test-output")

  implicit val output = new FileSystem(outputDir)

  val randomNumbers = new Producer[Iterable[Double]] {
    def get = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }
  }

  val cachedRandomNumbers = new CachedProducer[Iterable[Double]] {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }
  }

  "PipelineStep" should "regenerate on each invocation" in {
    randomNumbers.get should not equal (randomNumbers.get)

    val cached = randomNumbers.enableCaching

    cached.get should equal(cached.get)
  }

  "PersistentPipelineStep" should "read from file if exists" in {
    val pStep = randomNumbers.saveAsTsv("savedNumbers.txt")

    pStep.get should equal(pStep.get)

    val otherStep = cachedRandomNumbers.saveAsTsv("savedNumbers.txt")
    otherStep.get should equal(pStep.get)
  }

  "CachedStep" should "use cached value" in {
    cachedRandomNumbers.get should equal(cachedRandomNumbers.get)

    val uncached = cachedRandomNumbers.disableCaching

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedStep" should "read from file if exists" in {
    val pStep = cachedRandomNumbers.saveAsTsv("savedCachedNumbers.txt")

    pStep.get should equal(pStep.get)

    val otherStep = randomNumbers.saveAsTsv("savedCachedNumbers.txt")
    otherStep.get should equal(pStep.get)
  }

  override def beforeAll: Unit = {
    require((outputDir.exists && outputDir.isDirectory) || outputDir.mkdirs, s"Unable to create test output directory $outputDir")
  }
  override def afterAll: Unit = {
    FileUtils.deleteDirectory(outputDir)
  }
}
