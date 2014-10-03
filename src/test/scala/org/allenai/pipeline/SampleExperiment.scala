package org.allenai.pipeline

import org.allenai.common.testkit.{ScratchDirectory, UnitSpec}
import org.allenai.pipeline.IoHelpers._

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.DefaultJsonProtocol._

import scala.util.Random

import java.io.{InputStream, File}

/** Test PipelineRunner functionality */
class SampleExperiment extends UnitSpec
with BeforeAndAfterEach with BeforeAndAfterAll with ScratchDirectory {

  case class TrainedModel(info: String)

  object TrainedModel {
    val jsonFormat = jsonFormat1(apply)
  }

  class JoinAndSplitData(features: Producer[Iterable[Array[Double]]],
    labels: Producer[Iterable[Boolean]],
    testSizeRatio: Double)
      extends Producer[(Iterable[(Boolean, Array[Double])], Iterable[(Boolean,
          Array[Double])])] with Ai2CodeInfo {
    def create = {
      val rand = new Random
      val data = labels.get.zip(features.get)
      val testSize = math.round(testSizeRatio * data.size).toInt
      (data.drop(testSize), data.take(testSize))
    }

    override def signature = Signature.fromFields(this, "features", "labels", "testSizeRatio")
  }

  case class TrainModel(trainingData: Producer[Iterable[(Boolean, Array[Double])]])
      extends Producer[TrainedModel] with Ai2Signature {
    def create: TrainedModel = {
      val dataRows = trainingData.get
      train(dataRows) // Run training algorithm on training data
    }

    def train(data: Iterable[(Boolean, Array[Double])]): TrainedModel =
      TrainedModel(s"Trained model with ${data.size} rows")
  }

  type PRMeasurement = Iterable[(Double, Double, Double)]

  // Threshold, precision, recall
  case class MeasureModel(val model: Producer[TrainedModel],
    val testData: Producer[Iterable[(Boolean, Array[Double])]])
      extends Producer[PRMeasurement] with Ai2Signature {
    def create = {
      model.get
      // Just generate some dummy data
      val rand = new Random
      import math.exp
      var a = 0.0
      var b = 0.0
      for (i <- (0 until testData.get.size)) yield {
        val r = (exp(-a), 1 - exp(-b), exp(-b))
        a += rand.nextDouble * .03
        b += rand.nextDouble * .03
        r
      }
    }
  }

  case class ParsedDocument(info: String)

  case class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]])
      extends Producer[Iterable[Array[Double]]] with Ai2Signature {
    def create = {
      val features = for (doc <- documents.get) yield {
        val rand = new Random
        Array.fill(8)(rand.nextDouble)
      }
      features.toList
    }

  }

  object ParseDocumentsFromXML extends ArtifactIo[Iterator[ParsedDocument], StructuredArtifact]
  with Ai2CodeInfo {
    def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
      for ((id, is) <- a.reader.readAll) yield parse(id, is)
    }

    def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

    // Writing back to XML not supported
    def write(data: Iterator[ParsedDocument], artifact: StructuredArtifact) = ???

    override def toString = this.getClass.getSimpleName
  }

  class TrainModelPython(val data: Producer[FlatArtifact], val io: ArtifactIo[TrainedModel,
      FileArtifact])
      extends Producer[TrainedModel] with Ai2CodeInfo {
    def create: TrainedModel = {
      val inputFile = File.createTempFile("trainData", ".tsv")
      val outputFile = File.createTempFile("model", ".json")
      data.get.copyTo(new FileArtifact(inputFile))
      import sys.process._
      import scala.language.postfixOps
      // In real world, omit "echo"
      val stdout: String = s"echo train.py -input $inputFile -output $outputFile" !!
      // val model = io.read(new FileArtifact(outputFile)) // In real world, return this
      val model = TrainedModel(stdout)
      model
    }

    override def signature = Signature.fromFields(this, "data", "io")
  }


  val inputDir = new File("pipeline/src/test/resources/pipeline")
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import org.allenai.pipeline.IoHelpers._

  implicit val modelFormat = TrainedModel.jsonFormat

  implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
    tuple3ColumnFormat[Double, Double, Double](',')

  // Define our persistence implementation

  val input = new RelativeFileSystem(inputDir)

  "Sample Experiment" should "complete" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

    implicit val runner = PipelineRunner.writeToDirectory(scratchDir)

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs)

    // Define pipeline
    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val trainDataPersisted = Persist.Collection.asText(trainData)
    val model = Persist.Singleton.asJson(new TrainModel(trainDataPersisted))
    val measure: Producer[PRMeasurement] = Persist.Collection.asText(
      new MeasureModel(model, testData))
    runner.run(measure)

    assert(findFile(scratchDir, "JoinAndSplitData_1", ".txt"), "Training data file created")
    assert(findFile(scratchDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(scratchDir, "MeasureModel", ".txt"), "P/R file created")
    assert(findFile(scratchDir, "experiment", ".html"), "Experiment summary created")
  }

  "Subsequent Experiment" should "re-use existing data" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

    implicit val runner = PipelineRunner.writeToDirectory(scratchDir)

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val trainDataPersisted = Persist.Collection.asText(trainData)
    val model = Persist.Singleton.asJson(new TrainModel(trainDataPersisted))
    val measure: PersistedProducer[PRMeasurement, FlatArtifact] =
      Persist.Collection.asText(new MeasureModel(model, testData))
    val experimentSummary = runner.run(measure)

    val trainDataFile = new File(trainDataPersisted.artifact.url)
    val measureFile = new File(measure.artifact.url)

    val trainDataModifiedTime = trainDataFile.lastModified

    // Pipeline using different instances, with some shared steps
    val (trainDataFile2, measureFile2, experimentSummary2) = {
      val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
      val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
      val docFeatures = new FeaturizeDocuments(docs)

      val labelData: Producer[Iterable[Boolean]] =
        Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
      val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2)
      val trainDataPersisted = Persist.Collection.asText(trainData)
      val model = Persist.Singleton.asJson(new TrainModelPython(trainDataPersisted.asArtifact,
        SingletonIo.json[TrainedModel]))
      val measure: PersistedProducer[PRMeasurement, FlatArtifact] =
        Persist.Collection.asText(new MeasureModel(model, testData))
      val experimentSummary = runner.run(measure)
      (new File(trainDataPersisted.artifact.url), new File(measure.artifact.url),
          experimentSummary)
    }

    // Should use the same file to persist training data
    trainDataFile2 should equal(trainDataFile)
    // Should not recompute training data
    trainDataFile2.lastModified should equal(trainDataFile.lastModified)
    // Should store output in different location
    measureFile2 should not equal (measureFile)
    // Should store summary in different location
    experimentSummary2 should not equal (experimentSummary)

  }

  override def afterEach: Unit = {
    FileUtils.cleanDirectory(scratchDir)
  }

  def findFile(dir: File, prefix: String, suffix: String): Boolean =
    dir.listFiles.map(_.getName).exists(s => s.startsWith(prefix) && s.endsWith(suffix))

}
