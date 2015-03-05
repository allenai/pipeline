package org.allenai.pipeline

import org.allenai.common.testkit.{ScratchDirectory, UnitSpec}

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.DefaultJsonProtocol._

import scala.util.Random

import java.io.{File, InputStream}

/** See README.md for explanatory documentation of this example,
  * which runs a mocked-up pipeline to train and cross-validate a model.
  */
class SamplePipeline extends UnitSpec
with BeforeAndAfterEach with BeforeAndAfterAll with ScratchDirectory {

  case class TrainedModel(info: String)

  object TrainedModel {
    val jsonFormat = jsonFormat1(apply)
  }

  class JoinAndSplitData(
    features: Producer[Iterable[Array[Double]]],
    labels: Producer[Iterable[Boolean]],
    testSizeRatio: Double
  )
      extends Producer[(Iterable[(Boolean, Array[Double])], Iterable[(Boolean, Array[Double])])]
      with BasicPipelineStepInfo {
    def create = {
      val rand = new Random
      val data = labels.get.zip(features.get)
      val testSize = math.round(testSizeRatio * data.size).toInt
      (data.drop(testSize), data.take(testSize))
    }
  }

  case class TrainModel(trainingData: Producer[Iterable[(Boolean, Array[Double])]])
      extends Producer[TrainedModel] with BasicPipelineStepInfo {
    def create: TrainedModel = {
      val dataRows = trainingData.get
      train(dataRows) // Run training algorithm on training data
    }

    def train(data: Iterable[(Boolean, Array[Double])]): TrainedModel =
      TrainedModel(s"Trained model with ${data.size} rows")
  }

  type PRMeasurement = Iterable[(Double, Double, Double)]

  // Threshold, precision, recall
  class MeasureModel(
    val model: Producer[TrainedModel],
    val testData: Producer[Iterable[(Boolean, Array[Double])]]
  )
      extends Producer[PRMeasurement] with BasicPipelineStepInfo {
    def create = {
      model.get
      // Just generate some dummy data
      val rand = new Random
      import scala.math.exp
      var a = 0.0
      var b = 0.0
      for (i <- 0 until testData.get.size) yield {
        val r = (exp(-a), 1 - exp(-b), exp(-b))
        a += rand.nextDouble * .03
        b += rand.nextDouble * .03
        r
      }
    }
  }

  val inputDir = new File("src/test/resources/pipeline")
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import org.allenai.pipeline.IoHelpers._

  implicit val modelFormat = TrainedModel.jsonFormat

  implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] = tuple3ColumnFormat[Double, Double, Double](',')

  // Define our persistence implementation

  val input = new RelativeFileSystem(inputDir)
  implicit val output = new RelativeFileSystem(scratchDir)

  //    This also works:
  //      val s3Config = S3Config("ai2-pipeline-sample")
  //      val input = new RelativeFileSystem(inputDir)
  //      val output = new S3(s3Config, Some("test-output"))

  val pipeline = new Pipeline {
    override def artifactFactory = output
  }

  "Sample Pipeline 1" should "complete" in {
    // Read input data
    val featureData: Producer[Iterable[Array[Double]]] =
      Read.ArrayCollection.fromText[Double](input.flatArtifact(featureFile))
    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
    // Define pipeline
    val Producer2(trainData, testData) = new JoinAndSplitData(featureData, labelData, 0.2) -> (("train", "test"))
    val model: Producer[TrainedModel] =
      pipeline.Persist.Singleton.asJson(new TrainModel(trainData), Some("TrainModel.json"))
    val measure: Producer[PRMeasurement] =
      pipeline.Persist.Collection.asText(new MeasureModel(model, testData), Some("MeasureModel.txt"))

    // Run pipeline
    measure.get

    assert(findFile(scratchDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(scratchDir, "MeasureModel", ".txt"), "P/R file created")
  }

  case class ParsedDocument(info: String)

  case class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]])
      extends Producer[Iterable[Array[Double]]] with BasicPipelineStepInfo {
    def create = {
      val features = for (doc <- documents.get) yield {
        val rand = new Random
        Array.fill(8)(rand.nextDouble())
      }
      features.toList
    }
  }

  object ParseDocumentsFromXML extends ArtifactIo[Iterator[ParsedDocument], StructuredArtifact]
  with BasicPipelineStepInfo {
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
          Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
        // Define pipeline
        val Producer2(trainData: Producer[Iterable[(Boolean, Array[Double])]],
          testData: Producer[Iterable[(Boolean, Array[Double])]]) =
          new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
        val model: Producer[TrainedModel] =
          pipeline.Persist.Singleton.asJson(new TrainModel(trainData), Some("TrainModel.json"))
        val measure: Producer[PRMeasurement] =
          pipeline.Persist.Collection.asText(new MeasureModel(model, testData), Some("MeasureModel.txt"))
        measure.get

        assert(findFile(scratchDir, "TrainModel", ".json"), "Json file created")
        assert(findFile(scratchDir, "MeasureModel", ".txt"), "P/R file created")
  }

  class TrainModelPython(data: Producer[FlatArtifact], io: ArtifactIo[TrainedModel, FileArtifact])
      extends Producer[TrainedModel] with BasicPipelineStepInfo {
    def create: TrainedModel = {
      val inputFile = File.createTempFile("trainData", ".tsv")
      val outputFile = File.createTempFile("model", ".json")
      data.get.copyTo(new FileArtifact(inputFile))
      import scala.language.postfixOps
      import scala.sys.process._
      // In real world, omit "echo"
      val stdout: String = s"echo train.py -input $inputFile -output $outputFile" !!
      // val model = io.read(new FileArtifact(outputFile)) // In real world, return this
      val model = TrainedModel(stdout)
      model
    }
  }

  "Sample Pipeline 3" should "complete" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
//        implicit val featureFormat = columnArrayFormat[Double](',')
//        implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')
//
//        val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
//        val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
//        val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above
//
//        val labelData: Producer[Iterable[Boolean]] =
//          Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
//        // Define pipeline
//        val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
//        val trainingDataFile = pipeline.Persist.Collection.asText(trainData, Some("TrainingData.txt")).asArtifact
//        val model = pipeline.Persist.Singleton.asJson(new TrainModelPython(
//          trainingDataFile,
//          SingletonIo.json[TrainedModel]
//        ), "TrainModelPython.json")
//        val measure: Producer[PRMeasurement] = Persist.Collection.asText(
//          new MeasureModel(model, testData), "MeasureModel.txt"
//        )
//        measure.get
//
//        assert(findFile(scratchDir, "TrainingData", ".txt"), "Training data file created")
//        assert(findFile(scratchDir, "TrainModelPython", ".json"), "Json file created")
//        assert(findFile(scratchDir, "MeasureModel", ".txt"), "P/R file created")
  }

  override def afterEach(): Unit = {
    FileUtils.cleanDirectory(scratchDir)
  }

  def findFile(dir: File, prefix: String, suffix: String): Boolean =
    new File(dir, prefix + suffix).exists
}
