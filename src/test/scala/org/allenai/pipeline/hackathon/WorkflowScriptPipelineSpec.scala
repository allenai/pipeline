package org.allenai.pipeline.hackathon

import java.io.File
import java.net.URI
import org.allenai.common.Logging
import org.allenai.common.testkit.UnitSpec

class WorkflowScriptPipelineSpec extends UnitSpec {

  import CommandToken._

  val script = WorkflowScript(
    packages = Seq(
      Package(id = "scripts", source = new URI("./vision-py/scripts"))
    ),
    stepCommands = Seq(
      StepCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractArrows.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-py/png"), id = Some("pngDir")),
          StringToken("-o"),
          OutputDir("arrowDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractBlobs.py"),
          StringToken("-i"),
          ReferenceInput("pngDir"),
          StringToken("-o"),
          OutputDir("blobsDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractText.py"),
          StringToken("-i"),
          ReferenceInput("pngDir"),
          StringToken("-o"),
          OutputDir("textDir")
        )
      ),
      StepCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractRelations.py"),
          StringToken("-a"),
          ReferenceOutput("arrowDir"),
          StringToken("-b"),
          ReferenceOutput("blobsDir"),
          StringToken("-t"),
          ReferenceOutput("textDir"),
          StringToken("-o"),
          OutputDir("relationsDir")
        )
      )
    ),
    outputDir = new URI("s3://ai2-misc/hackathon-2015/pipeline")
  )

  "WorkflowScriptPipeline" should "work" in {
    val pipeline = new WorkflowScriptPipeline(script).buildPipeline
    pipeline.run("WOOOT!")
    // pipeline.dryRun(
    //   outputDir = new File("/Users/markschaake/tmp"),
    //   rawTitle = "Woohoo!"
    // )
  }
}

object WorflowScriptTester extends App with Logging {
  val test = new WorkflowScriptPipelineSpec
  val pipeline = new WorkflowScriptPipeline(test.script).buildPipeline
  pipeline.run("WOOOT!")
}
