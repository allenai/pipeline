package org.allenai.pipeline

import sun.management.FileSystem

import java.io.File
import java.lang.reflect.Field

import org.allenai.common.testkit.UnitSpec
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
class TestProducer extends UnitSpec with BeforeAndAfterAll {

  import scala.language.reflectiveCalls

  val rand = new Random

  import org.allenai.pipeline.IoHelpers._

  val outputDir = new File("test-output")

  implicit val output = new RelativeFileSystem(outputDir)

  val randomNumbers = new Producer[Iterable[Double]] with CachingDisabled {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this)
  }

  val cachedRandomNumbers = new Producer[Iterable[Double]] with CachingEnabled {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this)
  }

  "Uncached random numbers" should "regenerate on each invocation" in {
    randomNumbers.get should not equal (randomNumbers.get)

    val cached = randomNumbers.enableCaching

    cached.get should equal(cached.get)
  }

  "PersistedProducer" should "read from file if exists" in {
    val pStep = PersistedCollection.text("savedNumbers.txt")(randomNumbers)

    pStep.get should equal(pStep.get)

    val otherStep = PersistedCollection.text("savedNumbers.txt")(cachedRandomNumbers)
    otherStep.get should equal(pStep.get)
  }

  "CachedProducer" should "use cached value" in {
    cachedRandomNumbers.get should equal(cachedRandomNumbers.get)

    val uncached = cachedRandomNumbers.disableCaching

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedProducer" should "read from file if exists" in {
    val pStep = PersistedCollection.text("savedCachedNumbers.txt")(cachedRandomNumbers)

    pStep.get should equal(pStep.get)

    val otherStep = PersistedCollection.text("savedCachedNumbers.txt")(randomNumbers)
    otherStep.get should equal(pStep.get)
  }

  val randomIterator = new Producer[Iterator[Double]] {
    def create = {
      for (i <- (0 until 20).iterator) yield rand.nextDouble
    }

    def signature = Signature.fromFields(this)
  }

  "Random iterator" should "never cache" in {
    randomIterator.get.toList should not equal (randomIterator.get.toList)
  }

  "Persisted iterator" should "re-use value" in {
    val persisted = PersistedIterator.text("randomIterator.txt")(randomIterator)
    persisted.get.toList should equal(persisted.get.toList)
  }

  "Persisted iterator" should "read from file if exists" in {
    val persisted = PersistedIterator.text("savedCachedIterator.txt")(
      randomIterator.enableCaching)
    val otherStep = PersistedIterator.text("savedCachedIterator.txt")(
      randomIterator.disableCaching)
  }

  "Signatures" should "determine unique paths" in {
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    import Signature._

    implicit val runner = new SingleOutputDirPipelineRunner(new RelativeFileSystem(outputDir),
      "test-output")

    class RNG(val seed: Int, val length: Int) extends Producer[Iterable[Double]] {
      private val rand = new Random(seed)

      def create = (0 until length).map(i => rand.nextDouble)

      override def signature = Signature.fromFields(this, "seed", "length")
    }

    val rng1 = Persist.collection.asJson(new RNG(42, 100))
    val rng2 = Persist.collection.asJson(new RNG(117, 100))

    rng1.signature should not equal (rng2.signature)

    rng2.signature.toJson.convertTo[Signature] should equal(rng2.signature)

    val rng3 = Persist.collection.asJson(new RNG(42, 100))
    rng1.get should equal(rng3.get)
  }

  "PipelineRunner" should "run a pipeline" in {
    class RNG(val seed: Int, val length: Int) extends Producer[Iterable[Double]] {
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
