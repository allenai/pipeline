package org.allenai.pipeline.examples

import org.allenai.common.testkit.{UnitSpec, ScratchDirectory}
import org.allenai.pipeline.Pipeline

import java.io.File

/**
 * Created by rodneykinney on 8/23/15.
 */
class ExamplePipelineTest extends UnitSpec with ScratchDirectory {
  def validateOutput(pipeline: Pipeline): Unit = {
    val outputs = pipeline.persistedSteps.values
    outputs.size should be > 0
    outputs.foreach(s => assert(s.artifact.exists,s"Step $s not written"))
  }

}
