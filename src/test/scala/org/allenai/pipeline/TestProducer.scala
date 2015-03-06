package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

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

  val output = new RelativeFileSystem(outputDir)

  val pipeline = new Pipeline {
    override def artifactFactory = output
  }

  val randomNumbers = new Producer[Iterable[Double]] with CachingDisabled {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def stepInfo = PipelineStepInfo(className = "RNG")
  }

  val cachedRandomNumbers = new Producer[Iterable[Double]] with CachingEnabled {
    def create = {
      for (i <- (0 until 20)) yield rand.nextDouble
    }

    def stepInfo = PipelineStepInfo(className = "CachedRNG")
  }

  "Uncached random numbers" should "regenerate on each invocation" in {
    randomNumbers.get should not equal (randomNumbers.get)

    val cached = randomNumbers.withCachingEnabled

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

    val uncached = cachedRandomNumbers.withCachingDisabled

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedProducer" should "read from file if exists" in {
    val pStep = randomNumbers.persisted(
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

  val randomIterator = new Producer[Iterator[Double]] {
    def create = {
      for (i <- (0 until 20).iterator) yield rand.nextDouble
    }

    override def stepInfo = PipelineStepInfo(className = "RNG")
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
    val persisted = randomIterator.withCachingEnabled.persisted(
      LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt")
    )
    val otherStep = randomIterator.withCachingDisabled.persisted(
      LineIteratorIo.text[Double],
      output.flatArtifact("savedCachedIterator.txt")
    )
  }

  "Consumed iterator" should "be called only once" in {
    val numbers = randomIterator.get
    val consumedIterator = new Producer[Iterator[Double]] with BasicPipelineStepInfo {
      def create = {
        val n = numbers.toList
        n.toIterator
      }
    }

    consumedIterator.get.size should equal(20)
  }

  "Signature id" should "be order independent" in {
    val s1 = PipelineStepInfo("name", "version").addParameters("first" -> 1, "second" -> 2).signature
    val s2 = PipelineStepInfo("name", "version").addParameters("second" -> 2, "first" -> 1).signature
    val s3 = PipelineStepInfo("name", "version2").addParameters("first" -> 1, "second" -> 2).signature

    s1.id should equal(s2.id)

    s1.id should not equal (s3.id)
  }

  "Signature id" should "change with dependencies" in {
    val base = PipelineStepInfo("name", "version")
    val prs1 = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 1)
    }
    val prs1a = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 1)
    }
    val prs2 = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 2)
    }
    val prs3 = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 3, "upstream" -> prs1)
    }
    val prs4 = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 3, "upstream" -> prs2)
    }
    val prs5 = new PipelineStep {
      override def stepInfo = base.addParameters("param1" -> 3, "upstream" -> prs1a)
    }

    // parameters are different
    prs1.stepInfo.signature.id should not equal (prs2.stepInfo.signature.id)

    // parameters same, dependencies different
    prs3.stepInfo.signature.id should not equals (prs4.stepInfo.signature.id)

    // dependencies different instances with same signature
    prs5.stepInfo.signature.id should equal(prs3.stepInfo.signature.id)
  }

  "Signatures" should "determine unique paths" in {
    import spray.json._
    import DefaultJsonProtocol._

    class RNG(val seed: Int, val length: Int)
        extends Producer[Iterable[Double]] with BasicPipelineStepInfo {
      private val rand = new Random(seed)

      def create = (0 until length).map(i => rand.nextDouble)

      override def stepInfo =
        PipelineStepInfo.basic(this)
            .addFields(this, "seed", "length")
    }

    val rng1 = pipeline.Persist.Collection.asJson(new RNG(42, 100))
    val rng2 = pipeline.Persist.Collection.asJson(new RNG(117, 100))

    rng1.stepInfo.signature should not equal (rng2.stepInfo.signature)

    val rng3 = pipeline.Persist.Collection.asJson(new RNG(42, 100))
    rng1.get should equal(rng3.get)
  }

  case class CountDependencies(listGenerators: List[Producer[Iterable[Double]]])
      extends Producer[Int] with Ai2StepInfo {
    override def create: Int = listGenerators.size
  }

  "Signatures with dependencies in containers" should "identify dependencies" in {
    val has2 = new CountDependencies(List(randomNumbers, cachedRandomNumbers))
    val has3 = new CountDependencies(List(randomNumbers, cachedRandomNumbers, randomNumbers))

    has2.stepInfo.signature.dependencies.size should equal(2)
    has3.stepInfo.signature.dependencies.size should equal(3)

    has2.stepInfo.signature.id should not equal (has3.stepInfo.signature.id)
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

