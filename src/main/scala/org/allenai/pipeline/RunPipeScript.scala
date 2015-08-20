package org.allenai.pipeline

import java.io.{ File, PrintWriter }
import java.net.URI
import java.nio.file.Files

import org.allenai.common.{ Logging, Resource }
import org.allenai.pipeline.s3.S3Pipeline
import org.apache.commons.io.FileUtils

import scala.io.Source

/** Created by rodneykinney on 8/20/15.
  */
object RunPipeScript extends App with Logging {
  if (args.length == 0) {
    println("Usage: RunScript <script file> [output-url]")
    System.exit(1)
  }
  logger.info(s"Reading script file ${args(0)}")
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

  val script = PipeScriptCompiler.compileScript(scriptText)
  val interpreter = new PipeScriptInterpreter(S3Pipeline(outputUrl))
  val pipeline = interpreter.buildPipeline(script)
  val originalScriptUrl = {
    val upload = new ReplicateFile(Left((scriptFile, scriptUploadLocation _)))
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val portableScriptUrl = {
    val portableScriptText = interpreter.makePortable(script).scriptText
    val tmpDir = Files.createTempDirectory("pipeline").toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(tmpDir))
    val tmpFile = new File(tmpDir, scriptFile.getName())
    Resource.using(new PrintWriter(tmpFile)) {
      _.print(portableScriptText)
    }
    val upload = new ReplicateFile(Left((tmpFile, scriptUploadLocation _)))
    upload.get
    pipeline.toHttpUrl(upload.artifact.url)
  }

  val scripts = PipescriptSources(
    original = originalScriptUrl,
    portable = portableScriptUrl
  )
  pipeline.run(scriptFile.getName, Some(scripts))
  pipeline.openDiagram
}
