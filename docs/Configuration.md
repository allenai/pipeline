How to Control Pipelines in Configuration

AIP supports [Typesafe Config](https://github.com/typesafehub/config) to control persistence in a pipeline. Your Pipeline class should mix in the `ConfiguredPipeline` trait to enable Typesafe Config support, and set the `config` field to the desired config object. The structure of a pipeline config object is:

    my-pipeline-config {
      output {
        // The Root output URL of the pipeline
        dir = ...

        // Optional override for persistence of individual steps
        persist {
          // The key should equal the 'stepName' parameter
          // that is passed to one of the Pipeline.persist() methods
          Step1Name = ...
          // The value can be 'false' to disable persistence of a step
          // Or it can be an absolute URL specifying the output location
          Step2Name = ...
        }
      }

      // If 'true' then 'Pipeline.run()' will only perform a dry run
      dryRun = ...

      // The stepName for a single step in the pipeline, or a list of step names
      // Only the requested steps will be computed by `Pipeline.run()`
      runUntil = ...

      // Similar to runUntil, but an exception will be thrown if any direct inputs
      // to the requested steps do not exist
      runOnly = ...

    }

