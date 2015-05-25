package org.allenai.pipeline.spark.examples

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline.{CreateCoreArtifacts, ArtifactFactory, Pipeline}
import org.allenai.pipeline.spark.{SparkPipeline, CreateRddArtifacts, ReadObjectRddFromFiles}

import org.apache.spark.{SparkContext, SparkConf}

import java.io.File

/**
 * Created by rodneykinney on 5/24/15.
 */
object CountWordsAndLinesPipeline extends App {
  val sc = initSparkContext()

  val outputDir = new File("pipeline-output")

  val pipeline = {
    val artifactFactory = ArtifactFactory(CreateRddArtifacts.fromFileUrls, CreateCoreArtifacts.fromFileUrls)
    new Pipeline(outputDir.toURI, artifactFactory) with SparkPipeline {
      val sparkContext = sc
    }
  }

  // Define our input:  A collection of lines read from an inputFile
  val files = Read.Collection.fromText[String](new File("src/test/resources/paths.txt"))
  val lines = {
    val lineRdd = ReadObjectRddFromFiles[String](files, sc, Some(6))
    pipeline.persistRdd(lineRdd)
  }

  val countWords = {
    val count = CountWords(lines)
    implicit val format = tuple2ColumnFormat[String, Int]()
    pipeline.persistRdd(count)
  }

  val countLines = {
    val count = CountLines(lines)
    pipeline.Persist.Singleton.asText(count)
  }

  pipeline.run("CountWordsAndLines")

  def initSparkContext() = {
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "unit-test")
    new SparkContext(conf)
  }

}
