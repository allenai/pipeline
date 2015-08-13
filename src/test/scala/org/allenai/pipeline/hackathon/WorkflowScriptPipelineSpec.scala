package org.allenai.pipeline.hackathon

import java.io.File
import java.net.URI
import org.allenai.common.testkit.UnitSpec

class WorkflowScriptPipelineSpec extends UnitSpec {

  import CommandToken._

  "WorkflowScriptPipeline" should "work" in {
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
            Input(source = new URI("./vision-py/png"), id = Some("pngDir")),
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
            StringToken("--arrows"),
            ReferenceOutput("arrowDir"),
            StringToken("--blobs"),
            ReferenceOutput("blobsDir"),
            StringToken("--text"),
            ReferenceOutput("textDir"),
            StringToken("-o"),
            OutputDir("relationsDir")
          )
        )
      ),
      outputDir = new URI("s3://ai2-s2-dev/pipeline-hackathon/")
    )

    val pipeline = new WorkflowScriptPipeline(script).buildPipeline
    // pipeline.dryRun(
    //   outputDir = new File("/Users/markschaake/tmp"),
    //   rawTitle = "Woohoo!"
    // )
  }
}
