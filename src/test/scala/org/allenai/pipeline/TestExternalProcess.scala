package org.allenai.pipeline

import java.io.{ FileWriter, PrintWriter, File }

import org.allenai.common.Resource
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers.Read
import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._

import scala.io.Source

/** Created by rodneykinney on 5/14/15.
  */
class TestExternalProcess extends UnitSpec with ScratchDirectory {

  import ExternalProcess._

  "ExecuteShellCommand" should "return status code" in {
    val testTrue = new ExternalProcess("test", "a", "=", "a")
    testTrue.run().returnCode should equal(0)
    val testFalse = new ExternalProcess("test", "a", "=", "b")
    testFalse.run().returnCode should equal(1)
  }

  it should "create output files" in {
    val outputFile = new File(scratchDir, "testTouchFile/output")
    val outputArtifact = new FileArtifact(outputFile)
    val touchFile =
      RunExternalProcess.a("touch", OutputFileToken("target"))()
        .outputs("target").persisted(StreamIo, outputArtifact)
    touchFile.get
    outputFile should exist
  }

  it should "capture stdout" in {
    val echo = new ExternalProcess("echo", "hello", "world")
    val stdout = IOUtils.readLines(echo.run().stdout()).asScala.mkString("\n")
    stdout should equal("hello world")
  }
  it should "capture stderr" in {
    val noSuchParameter = new ExternalProcess("touch", "-x", "foo")
    val stderr = IOUtils.readLines(noSuchParameter.run().stderr()).asScala.mkString("\n")
    stderr.size should be > 0
  }
  it should "throw an exception if command is not found" in {
    val noSuchCommand = new ExternalProcess("eccho", "hello", "world")
    an[Exception] shouldBe thrownBy {
      noSuchCommand.run()
    }
  }
  it should "read input files" in {
    val dir = new File(scratchDir, "testCopy")
    dir.mkdirs()
    val inputFile = new File(dir, "input")
    val outputFile = new File(dir, "output")
    Resource.using(new PrintWriter(new FileWriter(inputFile))) {
      _.println("Some data")
    }
    val inputArtifact = new FileArtifact(inputFile)
    val outputArtifact = new FileArtifact(outputFile)

    val copy = RunExternalProcess.a("cp", InputFileToken("input"), OutputFileToken("output"))(
      inputs = Map("input" -> Read.fromArtifact(StreamIo, inputArtifact))
    )
      .outputs("output").persisted(StreamIo, outputArtifact)
    copy.get
    outputFile should exist
    Source.fromFile(outputFile).mkString should equal("Some data\n")

    copy.stepInfo.dependencies.size should equal(1)
    Workflow.upstreamDependencies(copy).size should equal(2)
    copy.stepInfo.dependencies.head._2.stepInfo.parameters("cmd") should equal("cp <input> <output>")
  }

  it should "pipe stdin to stdout" in {
    val echo = new ExternalProcess("echo", "hello", "world")
    val wc = new ExternalProcess("wc", "-c")
    val result = wc.run(stdinput = echo.run().stdout)
    IOUtils.readLines(result.stdout()).asScala.head.trim().toInt should equal(11)
  }
}
