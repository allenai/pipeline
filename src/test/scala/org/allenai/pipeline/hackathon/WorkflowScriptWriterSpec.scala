package org.allenai.pipeline.hackathon

import org.allenai.common.Logging
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

import java.io.File
import java.net.URI

class WorkflowScriptWriterSpec extends UnitSpec with ScratchDirectory {
  "WorkflowScriptWriter" should "work" in {
    //scratchDirectory
  }
}

object WorfklowScriptWriterTester extends App {
  val pipeline = WorkflowScriptPipeline.buildPipeline(TestData.script)
  WorkflowScriptWriter.write(TestData.script, pipeline, new File("/Users/markschaake/foo.pipe"))
}
