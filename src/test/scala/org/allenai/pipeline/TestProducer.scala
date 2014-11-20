package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import spray.json.DefaultJsonProtocol._

import scala.util.Random

import java.io.File

/** Created by rodneykinney on 8/19/14.
  */
// scalastyle:off magic.number
class TestProducer extends UnitSpec with BeforeAndAfterAll {

  import scala.language.reflectiveCalls

  val rand = new Random

  import org.allenai.pipeline.IoHelpers._

  val outputDir = new File("test-output-producer")

  implicit val output = new RelativeFileSystem(outputDir)

  implicit val runner = PipelineRunner.writeToDirectory(outputDir)

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
    val pStep = randomNumbers.persisted(
      LineCollectionIo.text[Double],
      output.flatArtifact("savedNumbers.txt")
    )

    pStep.get should equal(pStep.get)

    val otherStep = cachedRandomNumbers.persisted(
      LineCollectionIo.text[Double],
      new FileArtifact(new File(outputDir, "savedNumbers.txt"))
    )
    otherStep.get should equal(pStep.get)
  }

  "CachedProducer" should "use cached value" in {
    cachedRandomNumbers.get should equal(cachedRandomNumbers.get)

    val uncached = cachedRandomNumbers.disableCaching

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedProducer" should "read from file if exists" in {
    val pStep = cachedRandomNumbers.persisted(
      LineCollectionIo.text[Double],
      output.flatArtifact("savedCachedNumbers.txt")
    )

    pStep.get should equal(pStep.get)

    val otherStep = randomNumbers.persisted(
      LineCollectionIo.text[Double],
      output.flatArtifact("savedCachedNumbers.txt")
    )
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
    val persisted = randomIterator.persisted(
      LineIteratorIo.text[Double],
      output.flatArtifact("randomIterator.txt")
    )
    persisted.get.toList should equal(persisted.get.toList)
  }

  "Persisted iterator" should "read from file if exists" in {
    val persisted = randomIterator.enableCaching.persisted(
      LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt")
    )
    val otherStep = randomIterator.disableCaching.persisted(
      LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt")
    )
  }

  "Signature id" should "be order independent" in {
    val s1 = Signature("name", "version", "first" -> 1, "second" -> 2)
    val s2 = Signature("name", "version", "second" -> 2, "first" -> 1)
    val s3 = Signature("name", "version2", "first" -> 1, "second" -> 2)

    s1.id should equal(s2.id)

    s1.id should not equal (s3.id)
  }

  "Signature id" should "change with dependencies" in {
    val prs1 = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 1)

      override def outputLocation = None
    }
    val prs1a = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 1)

      override def outputLocation = None
    }
    val prs2 = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 2)

      override def outputLocation = None
    }
    val prs3 = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 3, "upstream" -> prs1)

      override def outputLocation = None
    }
    val prs4 = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 3, "upstream" -> prs2)

      override def outputLocation = None
    }
    val prs5 = new PipelineRunnerSupport with UnknownCodeInfo {
      override def signature = Signature("name", "version", "param1" -> 3, "upstream" -> prs1a)

      override def outputLocation = None
    }

    // parameters are different
    prs1.signature.id should not equal (prs2.signature.id)

    // parameters same, dependencies different
    prs3.signature.id should not equals (prs4.signature.id)

    // dependencies different instances with same signature
    prs5.signature.id should equal(prs3.signature.id)
  }

  "Signatures" should "determine unique paths" in {
    import org.allenai.pipeline.Signature._

    import spray.json._

    class RNG(val seed: Int, val length: Int)
        extends Producer[Iterable[Double]] with UnknownCodeInfo {
      private val rand = new Random(seed)

      def create = (0 until length).map(i => rand.nextDouble)

      override def signature = Signature.fromFields(this, "seed", "length")
    }

    val rng1 = Persist.Collection.asJson(new RNG(42, 100))
    val rng2 = Persist.Collection.asJson(new RNG(117, 100))

    rng1.signature should not equal (rng2.signature)

    val rng3 = Persist.Collection.asJson(new RNG(42, 100))
    rng1.get should equal(rng3.get)
  }

  case class CountDependencies(listGenerators: List[Producer[Iterable[Double]]])
      extends Producer[Int] with Ai2Signature {
    override def create: Int = listGenerators.size
  }

  "Signatures with dependencies in containers" should "identify dependencies" in {
    val has2 = new CountDependencies(List(randomNumbers, cachedRandomNumbers))
    val has3 = new CountDependencies(List(randomNumbers, cachedRandomNumbers, randomNumbers))

    has2.signature.dependencies.size should equal(2)
    has3.signature.dependencies.size should equal(3)

    has2.signature.id should not equal (has3.signature.id)
  }

  override def beforeAll: Unit = {
    require(
      (outputDir.exists && outputDir.isDirectory) || outputDir.mkdirs,
      s"Unable to create test output directory $outputDir"
    )
  }

  override def afterAll: Unit = {
    FileUtils.deleteDirectory(outputDir)
  }
}

