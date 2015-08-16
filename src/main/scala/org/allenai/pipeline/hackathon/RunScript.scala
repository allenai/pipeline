package org.allenai.pipeline.hackathon

import org.allenai.common.Resource
import org.allenai.pipeline.FlatArtifact
import org.allenai.pipeline.s3.S3Pipeline

import scala.io.Source

import java.io.{ File, PrintWriter }
import java.net.URI
import java.nio.file.Files

object RunScript extends App {
  if (args.length == 0) {
    println("Usage: RunScript <script file> [output-url]")
    System.exit(1)
  }
  val scriptFile = new File(args(0))

  val scriptText = Source.fromFile(scriptFile).mkString

  val outputUrl =
    if (args.length == 1) {
      new File(new File("pipeline-output"), "RunScript").toURI
    } else {
      new URI(args(1))
    }

  def scriptUploadLocation(name: String) =
    pipeline.artifactFactory.createArtifact[FlatArtifact](
      pipeline.rootOutputUrl, s"scripts/$name"
    )

  val script = new PipescriptCompiler().compileScript(scriptText)
  val interpreter = new PipescriptPipeline(S3Pipeline(outputUrl))
  val pipeline = interpreter.buildPipeline(script)
  val originalScriptUrl = {
    val upload = new ReplicateFile(Left((scriptFile, scriptUploadLocation _)))
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val stableScriptUrl = {
    val portableScriptText = interpreter.makePortable(script).scriptText
    val tmpFile = Files.createTempFile("portable", "pipe").toFile
    tmpFile.deleteOnExit()
    Resource.using(new PrintWriter(tmpFile)) {
      _.print(portableScriptText)
    }
    val upload = new ReplicateFile(Left((tmpFile, scriptUploadLocation _)))
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val scripts = PipescriptSources(
    original = originalScriptUrl,
    portable = stableScriptUrl
  )
  pipeline.run(scriptFile.getName, Some(scripts))
  pipeline.openDiagram
}
