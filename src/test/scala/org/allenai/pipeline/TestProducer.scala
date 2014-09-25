package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import spray.json.DefaultJsonProtocol._

import scala.util.Random

import java.io.File

/** Created by rodneykinney on 8/19/14.
  */
class TestProducer extends UnitSpec with BeforeAndAfterAll {

  import scala.language.reflectiveCalls

  val rand = new Random

  import org.allenai.pipeline.IoHelpers._

  val outputDir = new File("test-output")

  implicit val output = new RelativeFileSystem(outputDir)

  implicit val runner =  PipelineRunner.writeToDirectory(outputDir)

  val randomNumbers = new Producer[Iterable[Double]] with CachingDisabled with UnknownCodeInfo {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this).copy(name = "RNG")
  }

  val cachedRandomNumbers = new Producer[Iterable[Double]] with CachingEnabled with UnknownCodeInfo {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this).copy(name = "CachedRNG")
  }

  "Uncached random numbers" should "regenerate on each invocation" in {
    randomNumbers.get should not equal (randomNumbers.get)

    val cached = randomNumbers.enableCaching

    cached.get should equal(cached.get)
  }

  "PersistedProducer" should "read from file if exists" in {
    val pStep = randomNumbers.persisted(LineCollectionIo.text[Double],
      output.flatArtifact("savedNumbers.txt"))

    pStep.get should equal(pStep.get)

    val otherStep = cachedRandomNumbers.persisted(LineCollectionIo.text[Double],
      new FileArtifact(new File(outputDir, "savedNumbers.txt")))
    otherStep.get should equal(pStep.get)
  }

  "CachedProducer" should "use cached value" in {
    cachedRandomNumbers.get should equal(cachedRandomNumbers.get)

    val uncached = cachedRandomNumbers.disableCaching

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedProducer" should "read from file if exists" in {
    val pStep = cachedRandomNumbers.persisted(LineCollectionIo.text[Double],
      output.flatArtifact("savedCachedNumbers.txt"))

    pStep.get should equal(pStep.get)

    val otherStep = randomNumbers.persisted(LineCollectionIo.text[Double],
      output.flatArtifact("savedCachedNumbers.txt"))
    otherStep.get should equal(pStep.get)
  }

  val randomIterator = new Producer[Iterator[Double]] with UnknownCodeInfo {
    def create = {
      for (i <- (0 until 20).iterator) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this).copy(name = "RNG")
  }

  "Random iterator" should "never cache" in {
    randomIterator.get.toList should not equal (randomIterator.get.toList)
  }

  "Persisted iterator" should "re-use value" in {
    val persisted = randomIterator.persisted(LineIteratorIo.text[Double],
      output.flatArtifact("randomIterator.txt"))
    persisted.get.toList should equal(persisted.get.toList)
  }

  "Persisted iterator" should "read from file if exists" in {
    val persisted = randomIterator.enableCaching.persisted(LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt"))
    val otherStep = randomIterator.disableCaching.persisted(LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt"))
  }

  "Signatures" should "determine unique paths" in {
    import org.allenai.pipeline.Signature._

    import spray.json._

    class RNG(val seed: Int, val length: Int)
      extends PipelineStep[Iterable[Double]] with UnknownCodeInfo {
      private val rand = new Random(seed)

      def create = (0 until length).map(i => rand.nextDouble)

      override def signature = Signature.fromFields(this, "seed", "length").copy(name = "RNG")

    }

    val rng1 = Persist.Collection.asJson(new RNG(42, 100))
    val rng2 = Persist.Collection.asJson(new RNG(117, 100))

    rng1.signature should not equal (rng2.signature)

    val rng3 = Persist.Collection.asJson(new RNG(42, 100))
    rng1.get should equal(rng3.get)
  }

  "PipelineRunner" should "run a pipeline" in {
    class RNG(val seed: Int, val length: Int)
      extends PipelineStep[Iterable[Double]] with UnknownCodeInfo {
      private val rand = new Random(seed)

      def create = (0 until length).map(i => rand.nextDouble)

      override def signature: Signature = Signature.fromFields(this, "seed", "length")
    }

    val rng = new RNG(42, 100)

    rng.signature should not equal (null)

  }

  override def beforeAll: Unit = {
    require((outputDir.exists && outputDir.isDirectory) || outputDir.mkdirs,
      s"Unable to create test output directory $outputDir")
  }

  override def afterAll: Unit = {
    FileUtils.deleteDirectory(outputDir)
  }
}
