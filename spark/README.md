Spark Pipelines
=====

This project contains classes that support running pipelines with [Apache Spark](https://spark.apache.org/). To use AIP within Spark, you only need to include AIP as a library dependency and submit to a Spark environment using any of the standard mechanisms. Any Producer classes whose output type is `RDD[_]` will execute Spark jobs within that environment.

This project provides support primarily for persisting RDD outputs. By mixing the `SparkPipeline` trait into your pipeline class, you can use the `PersistRdd.asText()` or `asJson()` methods to save RDDs to Amazon S3 in the same way that `SparkContext.saveAsTextFile()` does: objects are converted to Strings and written, one line per object, into separate files, one file per partition.