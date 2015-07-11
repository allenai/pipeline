#Example Pipelines

Use `sbt "examples/runMain org.allenai.pipeline.examples.<example_name>"` to run the examples.

###CountWordsAndLinesPipeline
A basic pipeline using the simplest components and basic JSON serialization.

###TrainModelPipeline
A more complex workflow.  Demonstrates use of column-delimited serialization, multi-return-valued Producers,
custom serialization, and streaming data.

###TrainModelViaPythonPipeline
Demonstrates use of external processes within a pipeline.

