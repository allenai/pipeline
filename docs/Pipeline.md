How to Implement a Pipeline
=====


# The Pipeline class
To implement a pipeline, create a class that implements the `Pipeline` trait. It has one method you must implement: `rootOutputUrl`, which is an ordinary url pointing to the location where the pipeline will store its input.  A pipeline class can be configured to support different storage implementations. The base `Pipeline` trait only supports `file://` URLs. If the `S3Pipeline` trait is mixed into a pipeline class, it will support `s3://` URLs to store data in Amazon S3. To support a different storage implementation, see the documentation on [alternate storage](AlternateStorage.md)

# Adding steps to the pipeline
Any `Producer` instance can be added to a pipeline using one of the `persist()` methods. Common cases are supported via the `Pipeline.Persist` object, e.g. `Pipeline.Persist.Collection.asText[String]` is used to persist the output of a `Producer[String]` into a text file. Using one of the overridden `persist()` methods, you can specify a custom data format or override the output location.

# Running a pipeline
After all steps have been added to a pipeline, the `run()` method computes the output of all persisted steps. Data that exists already will not be recomputed.  It is possible that all outputs exist already, created by previous
runs by you or another user.  In that case, `Pipeline.run` will return immediately without performing
any calculations.

Before running a pipeline, you can call the `Pipeline.dryRun` method.  This will not perform any calculations,
but will output the summary HTML, allowing you to visualize your workflow before executing it. The HTML
will contain hyperlinks to the Signature-based path names where any output Artifacts will be written. Any outputs
that do not yet exist will be highlighted in red.  



