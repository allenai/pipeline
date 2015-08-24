Allen-AI Pipeline Scala API
=====

#Design Goals

Collaboration among data scientists is often complicated by the fact that individuals tend to
experiment with algorithms in isolated environments (typically their individual workstations). Sharing
data is difficult because there is no record of the code that was used to produce a given dataset
and there are no validation checks on the compatibility of code with the data format. Workflows with
any significant complexity become immediately unmanageable as soon as more than one scientist is involved.

There are many workflow management systems designed for production data pipelines. The problem of sharing data
is solved by providing a centralized execution environment, but users sacrifice the ability
to rapidly develop code that runs on their local machine while accessing production data.  To solve
these problems, AIP:

1.  Is a library that can be run locally or within a cloud environment
1.  Caches datasets for sharing between multiple users, even if they are running in separate environments
1.  Enforces compatibility of inputs/outputs at compile time
1.  Supports streaming calculations on out-of-RAM datasets
1.  Supports execution of arbitrary executables
1.  Support easy swapping of storage implementations

#Core Concepts

##Producer

A Producer[T] represents a calculation that produces an output of type T. It can have
arbitrary inputs, and any Producer of the same output type T is interchangeable. More details
are given [here](Producer.md)

##Artifact

A data structure saved in persistent storage is represented by the
Artifact trait.  An artifact may represent a flat file, a directory, a
zip archive, an S3 blob, an HDFS dataset, etc. Serialization/Deserialization of an object of type T into an artifact of type
A is represented by the ArtifactIo[T,A] trait. AIP provides implementations to serialize arbitrary
objects as column-delimited text or JSON (via spray.json)

##Persistence

A Producer[T] will create an in-memory object of type T.  This object can be passed to downstream consumers
without storing it to disk.  If desired, it can be written to disk before being passed downstream by
using one of the `Pipeline.Persist.*` methods.  These methods return a new Producer (actually an
instance of PersistedProducer) with the same output type T.  A PersistedProducer's `get` method
first checks to see whether data exists on disk. If the data exists, it will read the data from disk, and only
if the data does not exist will it compute the data. This allows caching of intermediate steps to speed up calculations.

AIP's persistence mechanism also allows users to reuse data from other users' pipeline.  Normally this is not
possible because file names are specified by users and they may collide.  To avoid this, a Pipeline
will choose a unique name for any Producer that it persists.  The name include a hash, which is based
on the parameters of the Producer and the parameters of all its transitive upstream ancestors.  By using
this hashing mechanism, different users running compatible code on different machines can share data
without fear of collision. The information that goes into determining the path name of a Producer's output is
encapsulated in a Signature object.

*AIP's persistence mechanism makes it 100% impossible to overwrite data.* Any data that exists on disk
will be used in place of recalculating it.  Only code that has not been executed before will result in
new data being written, and the path name chosen will always be unique.
This is true regardless of where the code was executed!

##Pipeline

A Pipeline has a `run` method that will calculate the result of any Producers that were persisted using
one of its `persist` methods.
When run, a Pipeline produces a static HTML visualization of the workflow, which includes hyperlinks
to the locations of any input/output/intermediate data Artifacts.  It also produces equivalent data in
JSON format that can be parsed programmatically.

Before running a pipeline, you can call the `Pipeline.dryRun` method.  This will not perform any calculations,
but will output the summary HTML, allowing you to visualize your workflow before executing it. The HTML
will contain hyperlinks to the Signature-based path names where any output Artifacts will be written. Any outputs
that do not yet exist will be highlighted in red.  It is possible that all outputs exist already, created by previous
runs by you or another user.  In that case, `Pipeline.run` will return immediately without performing
any calculations.

# Other Topics

1. [Configuration](Configuration.md)
1. [Out-of-RAM datasets](Streaming.md)
1. [Parallel Execution](ParallelExecution.md)
1. [Non-JVM processes](NonJVM.md)
1. [Using custom storage](AlternateStorage.md)

# Examples
Runnable sample code can be found in the [examples](../examples/README.md) project.

