package org.allenai.pipeline.examples

import org.allenai.pipeline.ExternalProcess._
import org.allenai.pipeline._

import java.io.File

/** Example pipeline that uses an external Python process
  * to train a model and to score the test data
  */
object TrainModelViaPythonPipeline extends App {
  import ExternalProcess._
  val inputDir = new File("../core/src/test/resources/pipeline")
  val pipeline = Pipeline(new File("pipeline-output"))

  // Create the training and test data
  val (trainData, testData) = TrainModelPipeline.produceTrainAndTestData(pipeline, inputDir)

  // Invoke an external Python process to train a model
  val trainModel =
    RunExternalProcess(
      "python",
      ScriptToken(new File(inputDir, "trainModel.py").getAbsolutePath),
      OutputFileToken("modelFile"),
      "-data",
      InputFileToken("trainingData")
    )(
        Seq(
          trainData
        ),
        versionHistory = Seq(
          "v1.0"
        )
      )

  // Capture the output of the process and persist it
  val modelFile = pipeline.persist(trainModel.outputs("modelFile"), StreamIo, "TrainedModel")

  val measureModel =
    RunExternalProcess(
      "python",
      ScriptToken(new File(inputDir, "scoreModel.py").getAbsolutePath),
      OutputFileToken("prFile"),
      "-model",
      InputFileToken("modelFile"),
      "-data",
      InputFileToken("testDataFile")
    )(
        inputs = Seq(
          modelFile,
          testData
        ),
        versionHistory = Seq(
          "v1.0"
        )
      )

  pipeline.persist(measureModel.outputs("prFile"), StreamIo, "PrecisionRecall")

  // Measure precision/recall of the model using the test data from above
  val steps = pipeline.run("Train Model Python")
  if (steps.isEmpty) throw new RuntimeException("Unsuccessful pipeline") // for unit test
}
