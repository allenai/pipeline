package org.allenai.pipeline.examples

import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._

import spray.json.DefaultJsonProtocol._

import java.io.File
import java.net.URI

/**
 * Created by rodneykinney on 5/16/15.
 */
object BasicPipeline extends App {
  val pipeline = Pipeline.saveToFileSystem(new File("pipeline-output"))

  val textFile = new File("src/test/resources/pipeline/features.txt")
  val lines = Read.Collection.fromText[String](textFile)
  val wordCount = {
    val count = CountWords(lines)
    pipeline.Persist.Singleton.asJson(count)
  }
  val lineCount = {
    val count = CountLines(lines)
    pipeline.Persist.Singleton.asJson(count)
  }

  pipeline.run("Count words and lines")
}



