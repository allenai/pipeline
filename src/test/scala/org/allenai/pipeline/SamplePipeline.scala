package org.allenai.pipeline

import java.io.{InputStream, File}

import org.allenai.common.testkit.UnitSpec
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

import scala.util.Random

/** See README.md for explanatory documentation of this example
  */
class SamplePipeline extends UnitSpec with BeforeAndAfterEach {

  case class TrainedModel(info: String)

  class JoinAndSplitData(features: Producer[Iterable[Array[Double]]], labels: Producer[Iterable[Boolean]], testSizeRatio: Double) extends Producer[(Iterable[(Boolean, Array[Double])], Iterable[(Boolean, Array[Double])])] {
    def create = {
      val rand = new Random
      val data = labels.get.zip(features.get)
      val testSize = math.round(testSizeRatio * data.size).toInt
      (data.drop(testSize), data.take(testSize))
    }
  }

  class TrainModel(trainingData: Producer[Iterable[(Boolean, Array[Double])]]) extends Producer[TrainedModel] {
    def create: TrainedModel = {
      val dataRows = trainingData.get
      train(dataRows) // Run training algorithm on training data
    }

    def train(data: Iterable[(Boolean, Array[Double])]): TrainedModel = TrainedModel(s"Trained model with ${data.size} rows")
  }

  type PRMeasurement = Iterable[(Double, Double, Double)]

  // Threshold, precision, recall
  class MeasureModel(model: Producer[TrainedModel], testData: Producer[Iterable[(Boolean, Array[Double])]]) extends Producer[PRMeasurement] {
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

  val outputDir = new File("pipeline/test-output")
  val inputDir = new File("pipeline/test-input")
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import spray.json.DefaultJsonProtocol._

  implicit val modelFormat = jsonFormat1(TrainedModel)

  import TsvFormats._

  implicit val prMeasurementFormat = tsvTuple3Format[Double, Double, Double](",")

  // Define our persistence implementation

  import IoHelpers._

  val input = new RelativeFileSystem(inputDir)
  val output = new RelativeFileSystem(outputDir)

  //    This also works:
  //    import org.jets3t.service.impl.rest.httpclient.RestS3Service
  //    import org.jets3t.service.model.S3Bucket
  //    import org.jets3t.service.security.AWSCredentials
  //    val accessKey: String = ???
  //    val secretAccessKey: String = ???
  //    val tmpDir: java.io.File = EnableWriteToS3.defaultTmpDir
  //    val s3Config = S3Config(new RestS3Service(new AWSCredentials(accessKey, secretAccessKey)), new S3Bucket("ai2-pipeline-sample"), tmpDir)
  //    val (input, output) = (new FileSystem(inputDir), new S3("test-output", s3Config)

  implicit val location = output

  "Sample Pipeline 1" should "complete" in {
    // Read input data
    val featureData: Producer[Iterable[Array[Double]]] = readTsvAsArrayCollection[Double](input.flatArtifact(featureFile))
    val labelData: Producer[Iterable[Boolean]] = readTsvAsCollection[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData: Producer[Iterable[(Boolean, Array[Double])]], testData: Producer[Iterable[(Boolean, Array[Double])]]) = new JoinAndSplitData(featureData, labelData, 0.2)
    val model: Producer[TrainedModel] = new TrainModel(trainData).saveAsJson("model.json")
    val measure: Producer[PRMeasurement] = new MeasureModel(model, testData).saveAsTsv("PR.txt")

    // Run pipeline
    measure.get

    assert(new File(outputDir, "model.json").exists, "Json file created")
    assert(new File(outputDir, "PR.txt").exists, "P/R file created")
  }

  case class ParsedDocument(info: String)

  class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]]) extends Producer[Iterable[Array[Double]]] {
    def create = {
      val features = for (doc <- documents.get) yield {
        val rand = new Random
        Array.fill(8)(rand.nextDouble)
      }
      features.toList
    }
  }

  object ParseDocumentsFromXML extends ArtifactIo[Iterator[ParsedDocument], StructuredArtifact] {
    def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
      for ((id, is) <- a.reader.readAll) yield parse(id, is)
    }

    def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

    // Writing back to XML not supported
    def write(data: Iterator[ParsedDocument], artifact: StructuredArtifact) = ???
  }

  "Sample Pipeline 2" should "complete" in {
    // Read input data
    val docDir = new File(inputDir, "xml")
    val docs = readFromArtifact(ParseDocumentsFromXML, new DirectoryArtifact(docDir))
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] = readTsvAsCollection[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData: Producer[Iterable[(Boolean, Array[Double])]], testData: Producer[Iterable[(Boolean, Array[Double])]]) = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val model: Producer[TrainedModel] = new TrainModel(trainData).saveAsJson("model.json")
    val measure: Producer[PRMeasurement] = new MeasureModel(model, testData).saveAsTsv("PR.txt")
    measure.get

    assert(new File(outputDir, "model.json").exists, "Json file created")
    assert(new File(outputDir, "PR.txt").exists, "P/R file created")
  }

  class TrainModelPython(data: Producer[FlatArtifact], io: ArtifactIo[TrainedModel, FileArtifact]) extends Producer[TrainedModel] {
    def create: TrainedModel = {
      val inputFile = File.createTempFile("trainData", ".tsv")
      val outputFile = File.createTempFile("model", ".json")
      data.get.copyTo(new FileArtifact(inputFile))
      import sys.process._
      import scala.language.postfixOps
      val stdout: String = s"echo train.py -input $inputFile -output $outputFile" !! // In real world, omit "echo"
      // val model = io.read(new FileArtifact(outputFile)) // In real world, return this
      val model = TrainedModel(stdout)
      model
    }
  }

  "Sample Pipeline 3" should "complete" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = tsvArrayFormat[Double](",")
    implicit val labelFeatureFormat = tsvTuple2Format[Boolean, Array[Double]]("\t")

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = readFromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] = readTsvAsCollection[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val trainingDataFile = trainData.saveAsTsv("trainData.tsv").asArtifact
    val model = new TrainModelPython(trainingDataFile, new JsonSingletonIo[TrainedModel]).saveAsJson("model.json")
    val measure: Producer[PRMeasurement] = new MeasureModel(model, testData).saveAsTsv("PR.txt")
    measure.get

    assert(new File(outputDir, "trainData.tsv").exists, "Training data file created")
    assert(new File(outputDir, "model.json").exists, "Json file created")
    assert(new File(outputDir, "PR.txt").exists, "P/R file created")
  }

  override def beforeEach: Unit = {
    require((outputDir.exists && outputDir.isDirectory) || outputDir.mkdirs, s"Unable to create test output directory $outputDir")
  }

  override def afterEach: Unit = {
    FileUtils.cleanDirectory(outputDir)
  }
}
