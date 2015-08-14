package org.allenai.pipeline.hackathon

import java.io.File
import java.net.URI

import scala.io.Source

object RunScript extends App {
  if (args.length == 0) {
    println("Usage: RunScript <script file> [output-url]")
    System.exit(1)
  }
  val scriptFile = new File(args(0))
  val visionScriptLines = Source.fromFile(scriptFile).getLines.toList

  val outputUrl =
    if (args.length == 1)
      new File(new File("pipeline-output"), "RunScript").toURI
    else
      new URI(args(1))
  val pipeline = WorkflowScriptPipeline.buildPipeline(outputUrl, visionScriptLines)
  val uploadedScriptUrl = {
    val upload = new ReplicateFile(scriptFile, None, outputUrl, pipeline.artifactFactory)
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }
  pipeline.run(scriptFile.getName, Some(uploadedScriptUrl))

}
