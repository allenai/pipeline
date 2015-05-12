package org.allenai.pipeline

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers._

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import spray.json.DefaultJsonProtocol._

import scala.util.Random

import java.io.{ File, InputStream }

/** Test Pipeline functionality */
class SamplePipeline extends UnitSpec
    with BeforeAndAfterEach with BeforeAndAfterAll with ScratchDirectory {

  import org.allenai.pipeline.SamplePipeline._

  val inputDir = new File("src/test/resources/pipeline")
  val outputDataDir = new File(scratchDir, "data")
  val input = new RelativeFileSystem(inputDir)
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import org.allenai.pipeline.IoHelpers._

  implicit val modelFormat = TrainedModel.jsonFormat

  implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
    tuple3ColumnFormat[Double, Double, Double](',')

  val pipeline = new Pipeline {
    def artifactFactory = new RelativeFileSystem(scratchDir)
  }

  "Sample Experiment" should "complete" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs)

    // Define pipeline
    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
    val trainDataPersisted = pipeline.Persist.Collection.asText(trainData, None, ".txt")
    val model = pipeline.Persist.Singleton.asJson(new TrainModel(trainDataPersisted), None, ".json")
    val measure: Producer[PRMeasurement] =
      pipeline.Persist.Collection.asText(new MeasureModel(model, testData), None, ".txt")
    pipeline.run("SamplePipeline")

    assert(findFile(outputDataDir, "JoinAndSplitData_train", ".txt"), "Training data file created")
    assert(findFile(outputDataDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(outputDataDir, "MeasureModel", ".txt"), "P/R file created")
    assert(findFile(new File(scratchDir, "summary"), "SamplePipeline", ".html"), "Experiment summary created")
  }

  "Subsequent Experiment" should "re-use existing data" in {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
    val trainDataPersisted = pipeline.Persist.Collection.asText(trainData, None, ".txt")
    val model = pipeline.Persist.Singleton.asJson(new TrainModel(trainDataPersisted), None, ".json")
    val measure =
      pipeline.Persist.Collection.asText(new MeasureModel(model, testData), None, ".txt")
    val experimentSummary = pipeline.run("Sample Experiment")

    val trainDataFile = new File(trainDataPersisted.artifact.url)
    val measureFile = new File(measure.artifact.url)

    val trainDataModifiedTime = trainDataFile.lastModified

    // Pipeline using different instances, with some shared steps
    val (trainDataFile2, measureFile2) = {
      val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
      val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
      val docFeatures = new FeaturizeDocuments(docs)

      val labelData: Producer[Iterable[Boolean]] =
        Read.Collection.fromText[Boolean](input.flatArtifact(labelFile))
      val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
      val trainDataPersisted = pipeline.Persist.Collection.asText(trainData, None, ".txt")
      val model = pipeline.Persist.Singleton.asJson(new TrainModelPython(
        trainDataPersisted,
        SingletonIo.json[TrainedModel]
      ), None, ".json")
      val measure: PersistedProducer[PRMeasurement, FlatArtifact] =
        pipeline.Persist.Collection.asText(new MeasureModel(model, testData))
      pipeline.run("SamplePipeline")
      (new File(trainDataPersisted.artifact.url),
        new File(measure.artifact.url))
    }

    // Should use the same file to persist training data
    trainDataFile2 should equal(trainDataFile)
    // Should not recompute training data
    trainDataFile2.lastModified should equal(trainDataFile.lastModified)
    // Should store output in different location
    measureFile2 should not equal (measureFile)
  }

  override def afterEach: Unit = {
    FileUtils.cleanDirectory(scratchDir)
  }

  def findFile(dir: File, prefix: String, suffix: String): Boolean =
    dir.listFiles.map(_.getName).exists(s => s.startsWith(prefix) && s.endsWith(suffix))

}

object SamplePipeline {

  case class TrainedModel(info: String)

  object TrainedModel {
    val jsonFormat = jsonFormat1(apply)
  }

  type TrainingPoint = (Boolean, Array[Double])

  case class JoinAndSplitData(
      features: Producer[Iterable[Array[Double]]],
      labels: Producer[Iterable[Boolean]],
      testSizeRatio: Double
  ) extends Producer[(Iterable[TrainingPoint], Iterable[TrainingPoint])] with Ai2StepInfo {
    def create: (Iterable[TrainingPoint], Iterable[TrainingPoint]) = {
      val rand = new Random
      val data = labels.get.zip(features.get)
      val testSize = math.round(testSizeRatio * data.size).toInt
      (data.drop(testSize), data.take(testSize))
    }

    override val description = "Join and split data."
  }

  case class TrainModel(trainingData: Producer[Iterable[TrainingPoint]])
      extends Producer[TrainedModel] with Ai2StepInfo {
    def create: TrainedModel = {
      val dataRows = trainingData.get
      train(dataRows) // Run training algorithm on training data
    }

    def train(data: Iterable[TrainingPoint]): TrainedModel =
      TrainedModel(s"Trained model with ${data.size} rows")

    override val description = "Train teh model.  Teh."
  }

  type PRMeasurement = Iterable[(Double, Double, Double)]

  // Threshold, precision, recall
  case class MeasureModel(
      val model: Producer[TrainedModel],
      val testData: Producer[Iterable[TrainingPoint]]
  ) extends Producer[PRMeasurement] with Ai2StepInfo {
    def create: PRMeasurement = {
      model.get
      // Just generate some dummy data
      val rand = new Random
      import scala.math.exp
      var a = 0.0
      var b = 0.0
      for (i <- (0 until testData.get.size)) yield {
        val r = (exp(-a), 1 - exp(-b), exp(-b))
        a += rand.nextDouble * .03
        b += rand.nextDouble * .03
        r
      }
    }

    override val description = "Measure the model."
  }

  case class ParsedDocument(info: String)

  case class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]])
      extends Producer[Iterable[Array[Double]]] with Ai2StepInfo {
    def create: Iterable[Array[Double]] = {
      val features = for (doc <- documents.get) yield {
        val rand = new Random
        Array.fill(8)(rand.nextDouble) // scalastyle:ignore
      }
      features.toList
    }
  }

  object ParseDocumentsFromXML extends ArtifactIo[Iterator[ParsedDocument], StructuredArtifact]
      with Ai2SimpleStepInfo {
    def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
      for ((id, is) <- a.reader.readAll) yield parse(id, is)
    }

    def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

    // Writing back to XML not supported
    def write(data: Iterator[ParsedDocument], artifact: StructuredArtifact): Unit = ???

    override def toString: String = this.getClass.getSimpleName
  }

  case class TrainModelPython(
    val data: PersistedProducer[Iterable[TrainingPoint], FileArtifact],
    val modelReader: DeserializeFromArtifact[TrainedModel, FileArtifact]
  )
      extends Producer[TrainedModel] with Ai2StepInfo {
    def create: TrainedModel = {
      val inputFile = data.artifact.asInstanceOf[FileArtifact].file
      val outputFile = File.createTempFile("model", ".json")
      import scala.language.postfixOps
      import scala.sys.process._
      // In real world, omit "echo"
      val stdout: String = s"echo train.py -input $inputFile -output $outputFile" !!
      //val model = modelReader.read(new FileArtifact(outputFile)) // In real world, return this
      val model = TrainedModel(stdout)
      model
    }
  }

}

/** An application that write out pipeline files for human viewing. */
object SamplePipelineApp extends App with Pipeline {

  import org.allenai.pipeline.SamplePipeline._

  val inputDir = new File("src/test/resources/pipeline")
  val outputDir = new File("pipeline-output")
  val artifactFactory = new RelativeFileSystem(outputDir)

  //  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  // Enable JSON serialization for our trained model object

  import org.allenai.pipeline.IoHelpers._

  implicit val modelFormat = TrainedModel.jsonFormat

  implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
    tuple3ColumnFormat[Double, Double, Double](',')

  // TSV format for label+features is <label><tab><comma-separated feature values>
  implicit val featureFormat = columnArrayFormat[Double](',')
  implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')

  val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
  val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
  val docFeatures = new FeaturizeDocuments(docs)

  // Define pipeline
  val labelData: Producer[Iterable[Boolean]] =
    Read.Collection.fromText[Boolean](new FileArtifact(new File(inputDir, labelFile)))
  val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
  val trainDataPersisted = Persist.Collection.asText(trainData, None, ".txt")
  val model = Persist.Singleton.asJson(new TrainModel(trainDataPersisted), None, ".json")
  val measure: Producer[PRMeasurement] =
    Persist.Collection.asText(new MeasureModel(model, testData), None, ".txt")
  run("Sample Pipeline")

  println(s"Pipeline files written to ${outputDir.getAbsolutePath}")
}
