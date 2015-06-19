#Allen-AI Pipeline Framework

The Allen-AI Pipeline (AIP) framework is a library that facilitates collaborative experimentation
by allowing users to define workflows that share data resources transparently while maintaining
complete freedom over the environment in which those workflows execute.

Send questions to *rodneyk@allenai.org*

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
arbitrary inputs, and any Producer of the same output type T is interchangeable.

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

#More Details

## Anatomy of a Producer

A Producer is conceptually a function that is decorated with enough metadata to build a Signature. Recall
that a Signature is used to determine a unique path name where the output of this Producer will be written.
The easiest way to define a Producer class is to make a case class that mixes in the Ai2StepInfo trait.
The only method that needs to be implemented in that case is the `create` method, which builds the output object
For example:

    case class CountLines(lines: Producer[Iterable[String]], countBlanks: Boolean = true) extends Producer[Int] with Ai2StepInfo {
      override protected def create: Int =
      if (countBlanks)
        lines.get.size
      else
        lines.get.filter(_.length > 0).size
    }

Notice how each Producer's `create` method calls the `get` method of its inputs. (`get` is simply an in-memory cache of
the result of `create`)  This is the mechanism by
which the end-to-end workflow is executed: the `Pipeline.run` method calls `get` on each persisted Producer.
The workflow graph is isomorphic to the object graph of Producers with references to other Producers.

The Signature of this Producer depends on the value of the `countBlanks` parameter, but also on the Signature of its
input, the Producer[Iterable[String]] whose lines it is counting.  That Producer's Signature depends likewise on
its own parameters and inputs, etc.  The outcome is that this Producer's output will be written to a
different location depending on where in a workflow it it plugged in.

Occasionally, it is necessary to change the logic of a Producer, such that its behavior will be different
from previous versions of the code.  The Signature includes a class-version field for this purpose. To indicate a change in the logic of a Producer, override the
`versionHistory` method.  For example:

    case class CountLines(lines: Producer[Iterable[String]], countBlanks: Boolean = true) extends Producer[Int] with Ai2StepInfo {
      override protected def create: Int =
        if (countBlanks)
          lines.get.size
        else
          lines.get.filter(_.trim.length > 0).size

      override def versionHistory = List(
        "v1.1" // Count whitespace-only lines as blank
      )
    }

In this way, cached data produced by older versions of your code can coexist with more recent versions.  Different
users can share data without conflict despite possibly running different versions of the code. (The value of the
version field can be any string, so long as it is unique.)

What happens if you change the logic of a Producer but forget to update the `versionHistory` method?
Even in this case, it is impossible to overwrite existing data.  Instead, your Producer may end up reading cached
data instead of recomputing based on the new logic.  To force a recomputation, you must change the Signature by updating the
`versionHistory` field.

##Dry Runs

Before running a pipeline, you can call the `Pipeline.dryRun` method.  This will not perform any calculations,
but will output the summary HTML, allowing you to visualize your workflow before executing it. The HTML
will contain hyperlinks to the Signature-based path names where any output Artifacts will be written. Any outputs
that do not yet exist will be highlighted in red.  It is possible that all outputs exist already, created by previous
runs by you or another user.  In that case, `Pipeline.run` will return immediately without performing
any calculations.

##Configuration

Use Typesafe Config to control persistence in a pipeline.

##Out-of-core Datasets

Have your Producers produce Iterators to stream data through a Pipeline.

##Parallel Execution

Have your Producers produce Futures to execute pipeline steps in parallel

##External Processes

Use the ExternalProcess class.

##Cloud Storage

S3 is supported out of the box via `Pipeline.saveToS3` factory method. Invoking this method
in place of `Pipeline.saveToFileSystem` is all that is needed to store your pipeline data
in the cloud.  A team of people running pipelines that save to the same directory within
S3 will be able to re-use each others' intermediate data as if they had computed it themselves.

##Alternate Storage

To persist data to other storage systems, cloud-based or otherwise,
implement an Artifact class and a corresponding ArtifactIo class.  You must also override `Pipeline.tryCreateArtifact`
to construct Artifacts of your custom type from relative paths.  With these in place, any Producers
persisted via `Pipeline.persist` will use your new storage mechanism.  Furthermore, you can change
the storage mechanism for an entire pipeline globally in one line of code by changing the implementation of
`Pipeline.tryCreateArtifact`.

#Example Pipelines

Use `sbt "examples/runMain org.allenai.pipeline.examples.<example_name>"` to run the examples.

###CountWordsAndLinesPipeline
A basic pipeline using the simples components and basic JSON serialization.

###TrainModelPipeline
A more complex workflow.  Demonstrates use of column-delimited serialization, multi-return-valued Producers,
custom serialization, and streaming data.

###TrainModelViaPythonPipeline
Demonstrates use of external processes within a pipeline.

#Summary

To summarize, the benefits provided by AIP are:

- Intermediate data is cached and is sharable by different users on different systems.
- A record of past runs is maintained, with navigable links to all inputs/output of the pipeline.
- A pipeline can be visualized before running.
- Output resource naming is managed to eliminate naming collisions.
- Input/output data is always compatible with the code reading the data.


















