package org.allenai.pipeline

import java.io.{ File, InputStream }

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers._
import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import spray.json.DefaultJsonProtocol._

import scala.util.Random

/** Test Pipeline functionality */
class SamplePipeline extends UnitSpec
    with BeforeAndAfterEach with BeforeAndAfterAll with ScratchDirectory {

  import org.allenai.pipeline.SamplePipeline._

  val inputDir = new File("src/test/resources/pipeline")
  val outputDataDir = new File(scratchDir, "data")
  val featureFile = "features.txt"
  val labelFile = "labels.txt"

  "Sample Experiment" should "complete" in {
    val pipeline = Pipeline(scratchDir)

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs)

    // Define pipeline
    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](new FileArtifact(new File(inputDir, labelFile)))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
    val trainDataPersisted = {
      // TSV format for label+features is <label><tab><comma-separated feature values>
      implicit val featureFormat = columnArrayFormat[Double](',')
      implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')
      pipeline.Persist.Collection.asText(trainData)
    }
    val model = {
      implicit val format = jsonFormat1(TrainedModel)
      pipeline.Persist.Singleton.asJson(new TrainModel(trainDataPersisted))
    }
    val measure: Producer[PRMeasurement] = {
      implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
        tuple3ColumnFormat[Double, Double, Double](',')
      pipeline.Persist.Collection.asText(new MeasureModel(model, testData))
    }
    pipeline.run("SamplePipeline")

    assert(findFile(outputDataDir, "JoinAndSplitData_train", ".txt"), "Training data file created")
    assert(findFile(outputDataDir, "TrainModel", ".json"), "Json file created")
    assert(findFile(outputDataDir, "MeasureModel", ".txt"), "P/R file created")
    assert(findFile(new File(scratchDir, "summary"), "SamplePipeline", ".html"), "Experiment summary created")
  }

  "Subsequent Experiment" should "re-use existing data" in {
    val pipeline = Pipeline(scratchDir)

    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
    val docFeatures = new FeaturizeDocuments(docs) // use in place of featureData above

    val labelData: Producer[Iterable[Boolean]] =
      Read.Collection.fromText[Boolean](new FileArtifact(new File(inputDir, labelFile)))
    val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
    val trainDataPersisted = {
      // TSV format for label+features is <label><tab><comma-separated feature values>
      implicit val featureFormat = columnArrayFormat[Double](',')
      implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')
      pipeline.Persist.Collection.asText(trainData)
    }
    val model = {
      implicit val format = jsonFormat1(TrainedModel)
      pipeline.Persist.Singleton.asJson(new TrainModel(trainDataPersisted))
    }
    val measure = {
      implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
        tuple3ColumnFormat[Double, Double, Double](',')
      pipeline.Persist.Collection.asText(new MeasureModel(model, testData))
    }
    val experimentSummary = pipeline.run("Sample Experiment")

    val trainDataFile = new File(trainDataPersisted.artifact.url)
    val measureFile = new File(measure.artifact.url)

    val trainDataModifiedTime = trainDataFile.lastModified

    // Pipeline using different instances, with some shared steps
    val (trainDataFile2, measureFile2) = {
      val pipeline = Pipeline(scratchDir)

      val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
      val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
      val docFeatures = new FeaturizeDocuments(docs)

      val labelData: Producer[Iterable[Boolean]] =
        Read.Collection.fromText[Boolean](new FileArtifact(new File(inputDir, labelFile)))
      val Producer2(trainData, testData) = new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
      val trainDataPersisted = {
        // TSV format for label+features is <label><tab><comma-separated feature values>
        implicit val featureFormat = columnArrayFormat[Double](',')
        implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')
        pipeline.Persist.Collection.asText(trainData)
      }
      val model = {
        implicit val format = jsonFormat1(TrainedModel)
        pipeline.Persist.Singleton.asJson(new TrainModelPython(
          trainDataPersisted,
          SingletonIo.json[TrainedModel]
        ))
      }
      val measure: PersistedProducer[PRMeasurement, FlatArtifact] = {
        implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
          tuple3ColumnFormat[Double, Double, Double](',')
        pipeline.Persist.Collection.asText(new MeasureModel(model, testData))
      }
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

  type TrainingPoint = (Boolean, Array[Double])

  case class JoinAndSplitData(
      features: Producer[Iterable[Array[Double]]],
      labels: Producer[Iterable[Boolean]],
      testSizeRatio: Double
  ) extends Producer[(Iterable[TrainingPoint], Iterable[TrainingPoint])] with Ai2StepInfo {
    def create: (Iterable[TrainingPoint], Iterable[TrainingPoint]) = {
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

  // Threshold, precision, recall
  type PRMeasurement = Iterable[(Double, Double, Double)]

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

  object ParseDocumentsFromXML extends Deserializer[Iterator[ParsedDocument], StructuredArtifact]
      with Ai2SimpleStepInfo {
    def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
      for ((id, is) <- a.reader.readAll) yield parse(id, is)
    }

    def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

    override def toString: String = this.getClass.getSimpleName
  }

  case class TrainModelPython[A <: FlatArtifact](
    val data: PersistedProducer[Iterable[TrainingPoint], A],
    val modelReader: Deserializer[TrainedModel, FileArtifact]
  )
      extends Producer[TrainedModel] with Ai2StepInfo {
    def create: TrainedModel = {
      val inputFile = File.createTempFile("trainingData", ".txt")
      data.artifact.copyTo(new FileArtifact(inputFile))
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
object SamplePipelineApp extends App {

  import org.allenai.pipeline.SamplePipeline._

  val inputDir = new File("src/test/resources/pipeline")
  val outputDir = new File("pipeline-output")
  val pipeline = Pipeline(outputDir)

  val labelFile = new File(inputDir, "labels.txt")

  // Read input documents
  val docs = {
    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
    Read.fromArtifact(ParseDocumentsFromXML, docDir)
  }

  // Compute document features
  val docFeatures =
    new FeaturizeDocuments(docs)

  // Read labels
  val labelData =
    Read.Collection.fromText[Boolean](new FileArtifact(labelFile))

  // Join labels with features and split into train/test
  val (trainData, testData) = {
    val joinSplit = new JoinAndSplitData(docFeatures, labelData, 0.2)
    val Producer2(train, test) = joinSplit -> (("train", "test"))
    val trainPersisted = {
      // TSV format for label+features is <label><tab><comma-separated feature values>
      implicit val featureFormat = columnArrayFormat[Double](',')
      implicit val labelFeatureFormat = tuple2ColumnFormat[Boolean, Array[Double]]('\t')
      pipeline.Persist.Collection.asText(train)
    }
    (trainPersisted, test)
  }

  // Train model
  val model = {
    val train = new TrainModel(trainData)
    implicit val format = jsonFormat1(TrainedModel)
    pipeline.Persist.Singleton.asJson(train)
  }

  // Measure test accuracy
  val measure = {
    val measure = new MeasureModel(model, testData)
    implicit val prMeasurementFormat: StringSerializable[(Double, Double, Double)] =
      tuple3ColumnFormat[Double, Double, Double](',')
    pipeline.Persist.Collection.asText(measure)
  }

  // Run the pipeline
  pipeline.run("Sample Pipeline")

  println(s"Pipeline files written to ${outputDir.getAbsolutePath}")
}
