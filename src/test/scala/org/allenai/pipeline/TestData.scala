package org.allenai.pipeline

object TestData {
  import java.net.URI

  import org.allenai.pipeline.PipeScript._
  import org.allenai.pipeline.PipeScript.CommandToken._

  val script = PipeScript(
    packages = Seq(
      Package(id = "scripts", source = new URI("./vision-example/scripts"))
    ),
    runCommands = Seq(
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractArrows.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-example/png")),
          StringToken("-o"),
          OutputDir("arrowDir")
        )
      ),
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractBlobs.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-example/png")),
          StringToken("-o"),
          OutputDir("blobsDir")
        )
      ),
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractText.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-example/png")),
          StringToken("-o"),
          OutputDir("textDir")
        )
      ),
      RunCommand(
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
    )
  )
}
