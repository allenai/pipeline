package org.allenai.pipeline.examples

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._

import spray.json.DefaultJsonProtocol._

import java.io.File

/** Example pipeline for the workflow of training a classifier
  * (The methods are just stubs to show the structure of such a pipeline.
  * No actual machine-learning code is included.)
  */
object TrainModelPipeline extends App {
  val inputDir = new File("../core/src/test/resources/pipeline")
  val pipeline = Pipeline(new File("pipeline-output"))

  // Create the training and test data
  val (trainData, testData) = produceTrainAndTestData(pipeline, inputDir)

  // Train the model
  val model = {
    // Save the model in Json format
    implicit val modelFormat = jsonFormat1(TrainedModel)
    pipeline.Persist.Singleton.asJson(TrainModel(trainData))
  }

  // Measure precision/recall of the model using the test data from above
  val measure = {
    // Save PR data in comma-separated text file
    implicit val prFormat = columnFormat3(PR, ',')
    pipeline.Persist.Collection.asText(MeasureModel(model, testData))
  }

  val steps = pipeline.run("Train Model")
  if (steps.isEmpty) throw new RuntimeException("Unsuccessful pipeline") // for unit test

  // Method that encapsulates a sub-pipeline that produces training and test data
  def produceTrainAndTestData(pipeline: Pipeline, inputDir: File) = {
    // Input documents.  Use custom IO class to parse XML
    val docs = {
      val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
      Read.fromArtifact(ParseDocumentsFromXML, docDir)
    }

    // Intermediate step to compute document features.  Not persisted
    val docFeatures = new FeaturizeDocuments(docs)

    // Read labels
    val labelData = Read.Collection.fromText[Boolean](new File(inputDir, "labels.txt"))

    // The JoinAndSplitData produces two outputs, which are assigned to separate variables
    val Producer2(trainData, testData) =
      new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))

    // The training data will be persisted, but
    val (trainDataPersisted, testDataPersisted) = {
      // Define TSV format for saving training data

      // Write features a comma-separated doubles
      implicit val featureFormat = columnArrayFormat[Double](',')

      // The final format is <label><tab><feature-list>
      implicit val trainingPointFormat = columnFormat2(TrainingPoint, '\t')
      (
        pipeline.Persist.Collection.asText(trainData, "TrainingData"),
        pipeline.Persist.Collection.asText(testData, "TestData")
      )
    }

    (trainDataPersisted, testDataPersisted)
  }

}
