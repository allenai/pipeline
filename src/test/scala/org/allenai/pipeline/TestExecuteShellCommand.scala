package org.allenai.pipeline

import java.io.{ FileWriter, PrintWriter, File }

import org.allenai.common.Resource
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }

import scala.io.Source

/** Created by rodneykinney on 5/14/15.
  */
class TestExecuteShellCommand extends UnitSpec with ScratchDirectory {

  import ExecuteShellCommand._

  "ExecuteShellCommand" should "return status code" in {
    val testTrue = new ExecuteShellCommand() {
      override def commandTokens: Seq[CommandToken] = List("test", "a", "=", "a")
    }
    testTrue.get.returnCode should equal(0)
    val testFalse = new ExecuteShellCommand() {
      override def commandTokens: Seq[CommandToken] = List("test", "a", "=", "b")
    }
    testFalse.get.returnCode should equal(1)
  }

  it should "create output files" in {
    val outputFile = new File(scratchDir, "testTouchFile/output")
    val outputArtifact = new FileArtifact(outputFile)
    val touchFile = new ExecuteShellCommand(
      outputs = List(OutputData("target", outputArtifact))
    ) {
      override def commandTokens: Seq[CommandToken] =
        List("touch", OutputFileToken("target"))
    }
    touchFile.get
    outputFile should exist
  }

  it should "capture stdout" in {
    val echo = new ExecuteShellCommand() {
      override def commandTokens: Seq[CommandToken] = List("echo", "hello", "world")
    }
    echo.get.stdout should equal("hello world")
  }
  it should "capture stderr" in {
    val noSuchParameter = new ExecuteShellCommand() {
      override def commandTokens: Seq[CommandToken] = List("touch", "-x", "foo")
    }
    noSuchParameter.get.stderr.size should be > 0
  }
  it should "throw an exception if command is not found" in {
    val noSuchCommand = new ExecuteShellCommand() {
      override def commandTokens: Seq[CommandToken] = List("eccho", "hello", "world")
    }
    an[Exception] shouldBe thrownBy {
      noSuchCommand.get
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

    val copy = new ExecuteShellCommand(
      inputs = List(InputData("input", inputArtifact)),
      outputs = List(OutputData("output", outputArtifact))
    ) {
      override def commandTokens: Seq[CommandToken] =
        List("cp", InputFileToken("input"), OutputFileToken("output"))
    }
    copy.get
    outputFile should exist
    Source.fromFile(outputFile).mkString should equal("Some data\n")

    copy.stepInfo.dependencies.size should equal(1)
    copy.stepInfo.parameters("cmd") should equal("cp <input> <output>")
  }
}
