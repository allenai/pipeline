package org.allenai.pipeline.hackathon

import org.allenai.pipeline._

import com.typesafe.config.ConfigFactory

import java.io.File
import scala.collection.mutable

class WorkflowScriptPipeline(script: WorkflowScript) {

  def buildPipeline: Pipeline = {
    val config = ConfigFactory.parseString(s"""
output.dir = "${script.outputDir}"
""")
    val pipeline = Pipeline.configured(config)

    val producers = mutable.Map[String, Producer[File]]()

    val runProcesses: Seq[RunProcess] = script.stepCommands map { stepCommand =>
      val args: Seq[ProcessArg] = stepCommand.tokens map {
        case CommandToken.Input(source, Some(id)) =>
          producers.get(id) match {
            case Some(producer) => InputFileArg(id, producer)
            case None =>
              val producer = ReadFromArtifact(
                UploadFile,
                pipeline.artifactFactory.createArtifact[FlatArtifact](source)
              )
              producers(id) = producer
              InputFileArg(id, producer)
          }

        case CommandToken.Input(source, None) =>
          val producer = ReadFromArtifact(
            UploadFile,
            pipeline.artifactFactory.createArtifact[FlatArtifact](source)
          )
          InputFileArg(source.toString, producer)

        // create a file producer from URI
        case CommandToken.OutputFile(id, _) => OutputFileArg(id)
        case CommandToken.OutputDir(id) => OutputDirArg(id)

        case CommandToken.StringToken(string) => StringArg(string)
      }
      val runProcess = RunProcess(args: _*)
      runProcess.outputFiles foreach {
        case (id, producer) =>
          producers(id) = producer
          pipeline.persist(producer, UploadFile, suffix = stepCommand.outputFiles(id).suffix)
      }
      runProcess.outputDirs foreach {
        case (id, producer) =>
          producers(id) = producer
          pipeline.persist(producer, UploadDirectory)
      }
      runProcess
    }

    // TODO build up the pipeline steps
    pipeline
  }

}
