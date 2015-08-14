package org.allenai.pipeline.hackathon

import org.allenai.common.Resource
import org.allenai.pipeline._

import java.io.File
import java.io.PrintWriter
import java.net.URI

object PipescriptWriter {
  val StableComment = "## stable ##"

  def write(script: Pipescript, pipeline: Pipeline, dest: File): Unit = {

    Resource.using(new PrintWriter(dest)) { writer =>
      writer.println(StableComment)
      writer.println("# This is an auto-generated AI2 Pipeline Workflow Script with stable inputs")
      writer.println()

      if (script.packages.nonEmpty) {
        writer.println("# Packaged directories containing pipeline resources:")
      }
      script.packages foreach { pkg =>
        val replicated = ReplicateDirectory(
          new File(pkg.source.getPath), None, pipeline.rootOutputUrl, pipeline.artifactFactory
        ).artifact.url
        writer.println(pkg.copy(source = replicated))
      }

      if (script.packages.nonEmpty) {
        writer.println("")
      }

      def isLocalUri(uri: URI): Boolean = Option(uri.getScheme) match {
        case Some("file") | None => true
        case _ => false
      }

      writer.println("# Pipeline steps:")
      script.stepCommands foreach { step =>
        val stringifiedTokens = step.tokens map {
          case dir @ CommandToken.InputDir(source) if isLocalUri(source) =>
            val replicated = ReplicateDirectory(
              new File(source.getPath), None, pipeline.rootOutputUrl, pipeline.artifactFactory
            ).artifact.url
            dir.copy(source = replicated).toString

          case file @ CommandToken.InputFile(source) if isLocalUri(source) =>
            val replicated = ReplicateFile(
              new File(source.getPath), None, pipeline.rootOutputUrl, pipeline.artifactFactory
            ).artifact.url
            file.copy(source = replicated).toString

          case other => other.toString
        }
        writer.println(stringifiedTokens.mkString(" "))
      }
    }
  }
}
