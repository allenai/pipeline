package org.allenai.pipeline.examples

class TestTrainModel extends ExamplePipelineTest {
   "pipeline" should "run" in {
     validateOutput(TrainModelPipeline.run(scratchDir))
   }
 }
