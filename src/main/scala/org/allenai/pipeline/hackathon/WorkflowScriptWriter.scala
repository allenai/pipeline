package org.allenai.pipeline.hackathon

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.net.URI
import org.allenai.common.Resource
import org.allenai.pipeline._


object WorkflowScriptWriter {
  def write(script: WorkflowScript, pipeline: Pipeline, dest: File): Unit = {
    def getStepInfos(step: PipelineStep): Seq[PipelineStepInfo] = {
      val stepInfo = step.stepInfo
      if (stepInfo.dependencies.isEmpty) {
        List(stepInfo)
      } else {
        stepInfo :: stepInfo.dependencies.values.flatMap(getStepInfos).toList
      }
    }

    val allSteps: Map[String, PipelineStepInfo] =
      (pipeline.persistedSteps.values.flatMap(getStepInfos) map { stepInfo =>
        (stepInfo.className, stepInfo)
      }).toMap

    Resource.using(new PrintWriter(dest)) { writer =>
      writer.println("# This is an AI2 Pipeline Workflow Script")
      writer.println()

      if (script.packages.nonEmpty) {
        writer.println("# Packaged directories containing pipeline resources:")
      }
      script.packages foreach { pkg =>
        val stepInfo = allSteps(pkg.source.toString)
        stepInfo.outputLocation match {
          case Some(out) => writer.println(pkg.copy(source = out).toString)
          case None => writer.println(pkg.toString)
        }
      }

      if (script.packages.nonEmpty) {
        writer.println("")
      }

      writer.println("# Pipeline steps:")
      script.stepCommands foreach { step =>
        val stringifiedTokens = step.tokens map {
          case dir: CommandToken.InputDir =>
            allSteps(dir.source.toString).outputLocation match {
              case Some(s3Uri) => dir.copy(source = s3Uri).toString
              case None => dir
            }

          case file: CommandToken.InputFile =>
            allSteps(file.source.toString).outputLocation match {
              case Some(s3Uri) => file.copy(source = s3Uri).toString
              case None => file
            }

          case other => other.toString
        }
        writer.println(stringifiedTokens.mkString(" "))
      }
    }
  }
}
