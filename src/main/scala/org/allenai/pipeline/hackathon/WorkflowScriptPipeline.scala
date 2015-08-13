package org.allenai.pipeline.hackathon

import com.typesafe.config.ConfigFactory
import org.allenai.pipeline._

class WorkflowScriptPipeline(script: WorkflowScript) {

  def buildPipeline: Pipeline = {
    val config = ConfigFactory.parseString(s"""
output.dir = "${script.outputDir}"
""")
    val pipeline = Pipeline.configured(config)

    // TODO build up the pipeline steps
    pipeline
  }
}
