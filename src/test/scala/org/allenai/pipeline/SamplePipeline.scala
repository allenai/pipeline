package org.allenai.pipeline

import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import java.io.{InputStream, File}

import org.allenai.common.testkit.UnitSpec
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Random

/** See README.md for explanatory documentation of this example,
  * which runs a mocked-up pipeline to train and cross-validate a model.
  */
class SamplePipeline extends UnitSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  case class TrainedModel(info: String)

  object TrainedModel {
    val jsonFormat = jsonFormat1(apply)
  }

  case class JoinAndSplitData(features: Producer[Iterable[Array[Double]]],
                              labels: Producer[Iterable[Boolean]],
                              testSizeRatio: Double)
    extends Producer[(Iterable[(Boolean, Array[Double])], Iterable[(Boolean, Array[Double])])]
    with UnknownCodeInfo {
    def create = {
      val rand = new Random
      val data = labels.get.zip(features.get)
      val testSize = math.round(testSizeRatio * data.size).toInt
      (data.drop(testSize), data.take(testSize))
    }

    def signature = Signature.fromObject(this)
  }

  case class TrainModel(trainingData: Producer[Iterable[(Boolean, Array[Double])]])
    extends Producer[TrainedModel] with UnknownCodeInfo {
    def create: TrainedModel = {
      val dataRows = trainingData.get
      train(dataRows) // Run training algorithm on training data
    }

    def signature = Signature.fromObject(this)

    def train(data: Iterable[(Boolean, Array[Double])]): TrainedModel =
      TrainedModel(s"Trained model with ${data.size} rows")
  }

  type PRMeasurement = Iterable[(Double, Double, Double)]

  // Threshold, precision, recall
  class MeasureModel(val model: Producer[TrainedModel],
                     val testData: Producer[Iterable[(Boolean, Array[Double])]])
    extends Producer[PRMeasurement] with UnknownCodeInfo {
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

    def signature = Signature.fromFields(this, "model", "testData")
  }

  val outputDir = new File("pipeline/test-output")
  val inputDir = new File("pipeline/src/test/resources/pipeline")
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import org.allenai.pipeline.IoHelpers._

  implicit val modelFormat = TrainedModel.jsonFormat

  implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] = tuple3ColumnFormat[Double, Double, Double](',')

  // Define our persistence implementation

  val input = new RelativeFileSystem(inputDir)
  implicit val output = new RelativeFileSystem(outputDir)

  //    This also works:
  //      val s3Config = S3Config("ai2-pipeline-sample")
  //      val input = new RelativeFileSystem(inputDir)
  //      val output = new S3(s3Config, Some("test-output"))

  "Sample Pipeline 1" should "complete" in {
    // Read input data
    val featureData: Producer[Iterable[Array[Double]]] =
      Read.arrayCollection.fromText[Double](input.flatArtifact(featureFile))
    val labelData: Producer[Iterable[Boolean]] =
      Read.collection.fromText[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData, testData) = new JoinAndSplitData(featureData, labelData, 0.2)
    val model: Producer[TrainedModel] =
      Persist.Singleton.asJson(new TrainModel(trainData), "TrainModel.json")
    val measure: Producer[PRMeasurement] =
      Persist.Collection.asText(new MeasureModel(model, testData), "MeasureModel.txt")

    // Run pipeline
    measure.get

    assert(findFile(outputDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(outputDir, "MeasureModel", ".txt"), "P/R file created")
  }

  case class ParsedDocument(info: String)

  case class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]])
    extends Producer[Iterable[Array[Double]]] with UnknownCodeInfo {
    def create = {
      val features = for (doc <- documents.get) yield {
        val rand = new Random
        Array.fill(8)(rand.nextDouble)
      }
      features.toList
    }

    def signature = Signature.fromObject(this)
  }

  object ParseDocumentsFromXML extends ArtifactIo[Iterator[ParsedDocument], StructuredArtifact]
  with UnknownCodeInfo {
    def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
      for ((id, is) <- a.reader.readAll) yield parse(id, is)
    }

    def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

    // Writing back to XML not supported
    def write(data: Iterator[ParsedDocument], artifact: StructuredArtifact) = ???

    override def toString = this.getClass.getSimpleName
  }

  "Sample Pipeline 2" should "complete" in {
    // Read input data
    val docDir = new File(inputDir, "xml")
    val docs = Read.fromArtifact(ParseDocumentsFromXML, new DirectoryArtifact(docDir))
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] =
      Read.collection.fromText[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData: Producer[Iterable[(Boolean, Array[Double])]],
    testData: Producer[Iterable[(Boolean, Array[Double])]]) =
      new JoinAndSplitData(docFeatures, labelData, 0.2)
    val model: Producer[TrainedModel] =
      Persist.Singleton.asJson(new TrainModel(trainData), "TrainModel.json")
    val measure: Producer[PRMeasurement] =
      Persist.Collection.asText(new MeasureModel(model, testData), "MeasureModel.txt")
    measure.get

    assert(findFile(outputDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(outputDir, "MeasureModel", ".txt"), "P/R file created")
  }

  case class TrainModelPython(data: Producer[FlatArtifact], io: ArtifactIo[TrainedModel,
    FileArtifact])
    extends Producer[TrainedModel] with UnknownCodeInfo {
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

    def signature = Signature.fromObject(this)
  }

  "Sample Pipeline 3" should "complete" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] =
      Read.collection.fromText[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val trainingDataFile = Persist.Collection.asText(trainData, "TrainingData.txt").asArtifact
    val model = Persist.Singleton.asJson(new TrainModelPython(trainingDataFile,
      SingletonIo.json[TrainedModel]), "TrainModelPython.json")
    val measure: Producer[PRMeasurement] = Persist.Collection.asText(
      new MeasureModel(model, testData), "MeasureModel.txt")
    measure.get

    assert(findFile(outputDir, "TrainingData", ".txt"), "Training data file created")
    assert(findFile(outputDir, "TrainModelPython", ".json"), "Json file created")
    assert(findFile(outputDir, "MeasureModel", ".txt"), "P/R file created")
  }

  override def beforeEach: Unit = {
    require((outputDir.exists && outputDir.isDirectory) ||
      outputDir.mkdirs, s"Unable to create test output directory $outputDir")
  }

  override def afterEach: Unit = {
    FileUtils.cleanDirectory(outputDir)
  }

  override def afterAll: Unit = {
    FileUtils.deleteDirectory(outputDir)
  }

  def findFile(dir: File, prefix: String, suffix: String): Boolean =
    dir.listFiles.map(_.getName).exists(s => s.startsWith(prefix) && s.endsWith(suffix))
}
