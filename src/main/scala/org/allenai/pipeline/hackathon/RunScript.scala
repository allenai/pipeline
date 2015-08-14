package org.allenai.pipeline.hackathon

import java.io.File
import java.nio.file.Files
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

  val script = new PipescriptCompiler().parseLines(outputUrl)(visionScriptLines)
  val pipeline = WorkflowScriptPipeline.buildPipeline(script)
  val originalScriptUrl = {
    val upload = new ReplicateFile(scriptFile, None, outputUrl, pipeline.artifactFactory)
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val stableScriptUrl = {
    val tmpDir = Files.createTempDirectory("pipescript")
    val stableTempFile = Files.createTempFile(tmpDir, "stable", ".pipe").toFile
    stableTempFile.deleteOnExit()
    tmpDir.toFile.deleteOnExit()
    WorkflowScriptWriter.write(script, pipeline, stableTempFile)
    val upload = new ReplicateFile(stableTempFile, None, outputUrl, pipeline.artifactFactory)
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val scripts = PipescriptSources(
    original = originalScriptUrl,
    stable = stableScriptUrl
  )
  pipeline.run(scriptFile.getName, Some(scripts))

}
