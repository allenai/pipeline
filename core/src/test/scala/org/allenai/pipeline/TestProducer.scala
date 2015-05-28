package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
// scalastyle:off magic.number
class TestProducer extends UnitSpec with ScratchDirectory {

  import scala.language.reflectiveCalls

  val rand = new Random

  import org.allenai.pipeline.IoHelpers._

  val outputDir = new File(scratchDir, "test-output-producer")

  val pipeline = Pipeline(outputDir)

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
    val pStep = pipeline.Persist.Collection.asText(randomNumbers)

    pStep.get should equal(pStep.get)

    val otherStep = cachedRandomNumbers.persisted(
      LineCollectionIo.text[Double],
      pStep.artifact
    )
    otherStep.get should equal(pStep.get)
  }

  "PersistedProducer" should "always read from file if caching disabled" in {
    val outputFile = new FileArtifact(new File(outputDir, "savedNumbersWithChanges.txt"))
    val io = LineCollectionIo.text[Double]
    val pStep = randomNumbers.persisted(
      io,
      outputFile
    ).withCachingDisabled

    val result1 = pStep.get

    val result2 = randomNumbers.get
    io.write(result2, outputFile)

    pStep.get should equal(result2)
  }

  "CachedProducer" should "use cached value" in {
    cachedRandomNumbers.get should equal(cachedRandomNumbers.get)

    val uncached = cachedRandomNumbers.withCachingDisabled

    uncached.get should not equal (uncached.get)
  }

  "PersistentCachedProducer" should "read from file if exists" in {
    val pStep = randomNumbers.persisted(LineCollectionIo.text[Double], new FileArtifact(new File(scratchDir, "rng.txt")))

    pStep.get should equal(pStep.get)

    val otherStep = randomNumbers.persisted(LineCollectionIo.text[Double], new FileArtifact(new File(scratchDir, "rng.txt")))
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
    val persisted = pipeline.Persist.Iterator.asText(randomIterator)
    persisted.get.toList should equal(persisted.get.toList)
  }

  "Persisted iterator" should "read from file if exists" in {
    val persisted = pipeline.Persist.Iterator.asText(randomIterator.withCachingEnabled)
    val otherStep = pipeline.persistToArtifact(
      randomIterator.withCachingDisabled,
      LineIteratorIo.text[Double],
      persisted.artifact,
      "RNG2"
    )
    persisted.get.toList should equal(otherStep.get.toList)
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

  "Signature id" should "change with differeing param values" in {
    prs1.stepInfo.signature.id should not equal (prs2.stepInfo.signature.id)
  }
  "Signature id" should "change with same params but different dependencies" in {
    prs3.stepInfo.signature.id should not equals (prs4.stepInfo.signature.id)
  }
  "Signature id" should "be the same if params and dependency signatures are the same" in {
    prs5.stepInfo.signature.id should equal(prs3.stepInfo.signature.id)
  }

  val prs6withDep = new PipelineStep {
    override def stepInfo: PipelineStepInfo = base.addParameters("param1" -> 4, "upstream" -> prs1)
  }
  val prs6withSomeDep = new PipelineStep {
    override def stepInfo: PipelineStepInfo =
      base.addParameters("param1" -> 4, "upstream" -> Some(prs1))
  }
  val prs6withSomeOtherDepSameSig = new PipelineStep {
    override def stepInfo: PipelineStepInfo =
      base.addParameters("param1" -> 4, "upstream" -> Some(prs1a))
  }
  val prs6withNoneDep = new PipelineStep {
    override def stepInfo: PipelineStepInfo = base.addParameters("param1" -> 4, "upstream" -> None)
  }
  val prs6withoutDep = new PipelineStep {
    override def stepInfo: PipelineStepInfo = base.addParameters("param1" -> 4)
  }
  "Signature with Option[Producer]" should "have the same signature as with just same Producer" in {
    prs6withDep.stepInfo.signature.id should equal(prs6withSomeDep.stepInfo.signature.id)
  }
  "Signature with Some(p1)" should "have same sig as with Some(p2) if p1.sig.id == p2.sig.id" in {
    prs6withDep.stepInfo.signature.id should equal(prs6withSomeOtherDepSameSig.stepInfo.signature.id)
  }
  "Signature with None as dep" should "have same sig as if dep not there at all" in {
    prs6withNoneDep.stepInfo.signature.id should equal(prs6withoutDep.stepInfo.signature.id)
  }
  "Signature with None as dep" should "have different sig as with Some" in {
    prs6withSomeDep.stepInfo.signature.id should not equal (prs6withNoneDep.stepInfo.signature.id)
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

    val rng1 = pipeline.Persist.Collection.asJson(new RNG(42, 100), "RNG1")
    val rng2 = pipeline.Persist.Collection.asJson(new RNG(117, 100), "RNG2")

    rng1.stepInfo.signature should not equal (rng2.stepInfo.signature)

    val rng3 = pipeline.Persist.Collection.asJson(new RNG(42, 100))
    rng1.get should equal(rng3.get)
  }

  case class CountDependencies(listGenerators: List[Producer[Iterable[Double]]])
      extends Producer[Int] with Ai2StepInfo {
    override def create: Int = listGenerators.size
  }

  case class CountDependenciesWithList(listGenerators: List[Producer[Iterable[Double]]], intList: Iterable[Int])
      extends Producer[Int] with Ai2StepInfo {
    override def create: Int = listGenerators.size
  }

  case class CountDependenciesWithOption(listGenerators: List[Producer[Iterable[Double]]], intOption: Option[Int])
      extends Producer[Int] with Ai2StepInfo {
    override def create: Int = listGenerators.size
  }

  "Signatures with dependencies in containers" should "identify dependencies" in {
    val has2 = new CountDependencies(List(randomNumbers, cachedRandomNumbers))
    val has2PlusList = new CountDependenciesWithList(List(randomNumbers, cachedRandomNumbers), List(1, 2, 3))
    val has2PlusOption = new CountDependenciesWithOption(List(randomNumbers, cachedRandomNumbers), Some(5))
    val has3 = new CountDependencies(List(randomNumbers, cachedRandomNumbers, randomNumbers))

    has2.stepInfo.signature.dependencies.size should equal(2)
    has2PlusList.stepInfo.signature.dependencies.size should equal(2)
    has2PlusOption.stepInfo.signature.dependencies.size should equal(2)
    has3.stepInfo.signature.dependencies.size should equal(3)

    has2.stepInfo.signature.id should not equal (has3.stepInfo.signature.id)
  }
}

