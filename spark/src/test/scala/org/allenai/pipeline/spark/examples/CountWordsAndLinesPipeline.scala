package org.allenai.pipeline.spark.examples

import java.io.File

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline.Pipeline
import org.allenai.pipeline.spark.{ ReadObjectRddFromFiles, SparkPipeline }
import org.apache.spark.{ SparkConf, SparkContext }

/** Created by rodneykinney on 5/24/15.
  */
object CountWordsAndLinesPipeline extends App {
  val sc = initSparkContext()

  val outputDir = new File("pipeline-output")

  val pipeline = {
    new Pipeline with SparkPipeline {
      override def rootOutputDataUrl = new File(new File(System.getProperty("user.dir")), "data").toURI
      override def rootOutputReportUrl = new File(new File(System.getProperty("user.dir")), "reports").toURI
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
    conf.set("spark.app.name", "sample-pipeline")
    new SparkContext(conf)
  }

}
