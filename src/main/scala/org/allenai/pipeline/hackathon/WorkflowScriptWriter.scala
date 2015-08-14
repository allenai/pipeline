package org.allenai.pipeline.hackathon

import org.allenai.common.Resource
import org.allenai.pipeline._

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.net.URI

object WorkflowScriptWriter {
  def write(script: WorkflowScript, pipeline: Pipeline, dest: File): Unit = {

    Resource.using(new PrintWriter(dest)) { writer =>
      writer.println("# This is an AI2 Pipeline Workflow Script")
      writer.println()

      if (script.packages.nonEmpty) {
        writer.println("# Packaged directories containing pipeline resources:")
      }
      script.packages foreach { pkg =>
        val replicated = ReplicateDirectory(
          new File(pkg.source), None, pipeline.rootOutputUrl, pipeline.artifactFactory
        ).get.toURI
        writer.println(pkg.copy(source = replicated))
      }

      if (script.packages.nonEmpty) {
        writer.println("")
      }

      writer.println("# Pipeline steps:")
      // script.stepCommands foreach { step =>
      //   val stringifiedTokens = step.tokens map {
      //     case dir: CommandToken.InputDir =>
      //       allSteps(dir.source.toString).outputLocation match {
      //         case Some(s3Uri) => dir.copy(source = s3Uri).toString
      //         case None => dir
      //       }

      //     case file: CommandToken.InputFile =>
      //       allSteps(file.source.toString).outputLocation match {
      //         case Some(s3Uri) => file.copy(source = s3Uri).toString
      //         case None => file
      //       }

      //     case other => other.toString
      //   }
      //   writer.println(stringifiedTokens.mkString(" "))
      //}
    }
  }
}
