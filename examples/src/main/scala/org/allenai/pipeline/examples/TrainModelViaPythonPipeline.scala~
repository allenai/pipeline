package org.allenai.pipeline.examples

import org.allenai.pipeline._

import java.io.File

/** Example pipeline that uses an external Python process
  * to train a model and to score the test data
  */
object TrainModelViaPythonPipeline extends App {
  import org.allenai.pipeline.IoHelpers._
  val inputDir = new File("../core/src/test/resources/pipeline")
  val pipeline = Pipeline(new File("pipeline-output"))

  // Create the training and test data
  val (trainData, testData) = TrainModelPipeline.produceTrainAndTestData(pipeline, inputDir)

  // Invoke an external Python process to train a model
  val trainModel =
    RunProcess(
      "python",
      "trainModel.py",
      OutputFileArg("modelFile"),
      "-data",
      trainData -> "trainingData"
    )

  // Capture the output of the process and persist it
  val modelFile = pipeline.persist(trainModel.outputFiles("modelFile"), UploadFile, "TrainedModel")

  val measureModel =
    RunProcess(
      "python",
      "scoreModel.py",
      OutputFileArg("prFile"),
      "-model",
      modelFile -> "modelFile",
      "-data",
      testData -> "testDataFile"
    )

  pipeline.persist(measureModel.outputFiles("prFile"), UploadFile, "PrecisionRecall")

  // Measure precision/recall of the model using the test data from above
  val steps = pipeline.run("Train Model Python")
  if (steps.isEmpty) throw new RuntimeException("Unsuccessful pipeline") // for unit test
}
