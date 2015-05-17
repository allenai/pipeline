package org.allenai.pipeline.examples

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._

import spray.json.DefaultJsonProtocol._

import java.io.File

/**
 * Created by rodneykinney on 5/17/15.
 */
object TrainModelPipeline extends App {
  val inputDir = new File("src/test/resources/pipeline")
  val labelFile = new File(inputDir, "labels.txt")

  val pipeline = Pipeline.saveToFileSystem(new File("pipeline-output"))

  val docDir = new DirectoryArtifact(new File(inputDir, "xml"))
  val docs = Read.fromArtifact(ParseDocumentsFromXML, docDir)
  val docFeatures = new FeaturizeDocuments(docs)

  // Define pipeline
  val labelData = Read.Collection.fromText[Boolean](labelFile)
  val Producer2(trainData, testData) =
    new JoinAndSplitData(docFeatures, labelData, 0.2) -> (("train", "test"))
  val trainDataPersisted = {
    // TSV format for label+features is <label><tab><comma-separated feature values>
    implicit val featureFormat = columnArrayFormat[Double](',')
    implicit val trainingPointFormat = columnFormat2(TrainingPoint, '\t')
    pipeline.Persist.Collection.asText(trainData)
  }
  val model = {
    implicit val modelFormat = jsonFormat1(TrainedModel)
    pipeline.Persist.Singleton.asJson(TrainModel(trainDataPersisted))
  }
  val measure = {
    implicit val prFormat = columnFormat3(PR, ',')
    pipeline.Persist.Collection.asText(MeasureModel(model, testData))
  }
  pipeline.run("Train Model")

}
