package org.allenai.pipeline.examples

import org.allenai.pipeline.IoHelpers._
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
    RunProcess(
      "python",
      "script" -> new File(inputDir, "trainModel.py"),
      OutputFileArg("modelFile"),
      "-data",
      "trainingData" -> trainData
    )

  // Capture the output of the process and persist it
  val modelFile = pipeline.persist(trainModel.outputFiles("modelFile"), UploadFile)

  val measureModel =
    RunProcess(
      "python",
      "script" -> new File(inputDir, "scoreModel.py"),
      OutputFileArg("prFile"),
      "-model",
      "modelFile" -> modelFile,
      "-data",
      "testDataFile" -> testData
    )

  pipeline.persist(measureModel.outputFiles("prFile"), UploadFile, "PrecisionRecall")

  // Measure precision/recall of the model using the test data from above
  pipeline.run("Train Model Python")

}
