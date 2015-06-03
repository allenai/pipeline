package org.allenai.pipeline.examples

import org.allenai.pipeline.ExternalProcess._
import org.allenai.pipeline._

import java.io.File

/** Example pipeline that uses an external Python process
  * to train a model and to score the test data
  */
object TrainModelViaPythonPipeline extends App {
  val inputDir = new File("src/test/resources/pipeline")
  val pipeline = Pipeline(new File("pipeline-output"))

  // Create the training and test data
  val (trainData, testData) = TrainModelPipeline.produceTrainAndTestData(pipeline, inputDir)

  // Invoke an external Python process to train a model
  val trainModel =
    RunExternalProcess(
      "python",
      InputFileToken("script"),
      OutputFileToken("modelFile"),
      "-data",
      InputFileToken("trainingData")
    )(inputs =
        Map("trainingData" -> trainData, "script" -> new FileArtifact(new File(inputDir, "trainModel.py"))))

  // Capture the output of the process and persist it
  val modelFile = pipeline.persist(trainModel.outputs("modelFile"), StreamIo, "TrainedModel")

  val measureModel =
    RunExternalProcess(
      "python",
      InputFileToken("script"),
      OutputFileToken("prFile"),
      "-model",
      InputFileToken("modelFile"),
      "-data",
      InputFileToken("testDataFile")
    )(inputs = Map(
        "script" -> new FileArtifact(new File(inputDir, "scoreModel.py")),
        "modelFile" -> modelFile,
        "testDataFile" -> testData
      ))

  pipeline.persist(measureModel.outputs("prFile"), StreamIo, "PrecisionRecall")

  // Measure precision/recall of the model using the test data from above
  pipeline.run("Train Model Python")

}
