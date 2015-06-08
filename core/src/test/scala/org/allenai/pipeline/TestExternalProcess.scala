package org.allenai.pipeline

import org.allenai.common.Resource
import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.allenai.pipeline.IoHelpers.Read

import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.io.Source

import java.io.{ File, FileWriter, InputStream, PrintWriter }
import java.lang.Thread.UncaughtExceptionHandler

/** Created by rodneykinney on 5/14/15.
  */
class TestExternalProcess extends UnitSpec with ScratchDirectory {

  import org.allenai.pipeline.ExternalProcess._

  "ExecuteShellCommand" should "return status code" in {
    val testTrue = new ExternalProcess("test", "a", "=", "a")
    testTrue.run(Seq()).returnCode should equal(0)
    val testFalse = new ExternalProcess("test", "a", "=", "b")
    testFalse.run(Seq()).returnCode should equal(1)
  }

  it should "create output files" in {
    val outputFile = new File(scratchDir, "testTouchFile/output")
    val outputArtifact = new FileArtifact(outputFile)
    val touchFile =
      new ProducerWithPersistence(RunExternalProcess("touch", OutputFileToken("target"))(Seq())
        .outputs("target"), StreamIo, outputArtifact)
    touchFile.get
    outputFile should exist
  }

  def ppVersionHistTest(pipeline: Pipeline, vh1: Seq[String]): (PersistedProducer[() => InputStream, FlatArtifact]) =
    {
      val touchFile1 =
        RunExternalProcess("touch", OutputFileToken("target"))(Seq(),
          versionHistory = vh1
        ).outputs("target")
      pipeline.persist(touchFile1, StreamIo, s"${touchFile1.stepInfo.signature.id}")
    }

  it should "cache when versionHistory is equal" in {
    val pipeline = Pipeline(new File(scratchDir, "testVersionHistory"))
    val pp1 = ppVersionHistTest(pipeline, Seq("v1.0"))
    val pp2 = ppVersionHistTest(pipeline, Seq("v1.0"))
    pipeline.run("versioning test")

    pp1.artifact.url.getRawPath() should equal(pp2.artifact.url.getRawPath())
  }

  it should "recompute when versionHistory is unequal" in {
    val pipeline = Pipeline(new File(scratchDir, "testVersionHistory"))
    val pp1 = ppVersionHistTest(pipeline, Seq("v1.0"))
    val pp2 = ppVersionHistTest(pipeline, Seq("v1.1"))
    pipeline.run("versioning test")

    pp1.artifact.url.getRawPath() should not equal (pp2.artifact.url.getRawPath())
  }

  it should "only account for the last element of versionHistory" in {
    val pipeline = Pipeline(new File(scratchDir, "testVersionHistory"))
    val pp1 = ppVersionHistTest(pipeline, Seq("v1.1"))
    val pp2 = ppVersionHistTest(pipeline, Seq("v1.0", "v1.1"))
    pipeline.run("versioning test")

    pp1.artifact.url.getRawPath() should equal(pp2.artifact.url.getRawPath())
  }

  it should "account for the last element of versionHistory" in {
    val pipeline = Pipeline(new File(scratchDir, "testVersionHistory"))
    val pp1 = ppVersionHistTest(pipeline, Seq("v1.1"))
    val pp2 = ppVersionHistTest(pipeline, Seq("v1.1", "v1.2"))
    pipeline.run("versioning test")

    pp1.artifact.url.getRawPath() should not equal (pp2.artifact.url.getRawPath())
  }

  def CatFileForTest(scratchDir: File, rfile: String, vh1: Seq[String]): Producer[() => InputStream] = {
    val fa = new FileArtifact(new File(scratchDir, rfile))
    RunExternalProcess("cat", InputFileToken("target"))(Seq(fa),
      versionHistory = vh1
    ).stdout
  }

  "RunExternalProcess signature" should "be invariant between identical producers" in {
    val rfile = "in1.txt"
    Resource.using(new java.io.PrintWriter(new java.io.FileWriter(new File(scratchDir, rfile)))) {
      _.println("hello")
    }
    val pipeline = Pipeline(new File(scratchDir, "ExtargSignature"))
    val cat1 = CatFileForTest(scratchDir, rfile, List("v1.0"))
    val cat2 = CatFileForTest(scratchDir, rfile, List("v1.0"))
    val sig1 = cat1.stepInfo.signature.infoString
    val sig2 = cat2.stepInfo.signature.infoString
    sig1 should equal(sig2)
  }

  it should "capture stdout" in {
    val echo = new ExternalProcess("echo", "hello", "world")
    val stdout = IOUtils.readLines(echo.run(Seq()).stdout()).asScala.mkString("\n")
    stdout should equal("hello world")
  }
  it should "capture stderr" in {
    val noSuchParameter = new ExternalProcess("touch", "-x", "foo")
    val stderr = IOUtils.readLines(noSuchParameter.run(Seq()).stderr()).asScala.mkString("\n")
    stderr.size should be > 0
  }
  it should "throw an exception if command is not found" in {
    val noSuchCommand = new ExternalProcess("eccho", "hello", "world")
    val defaultHandler = Thread.getDefaultUncaughtExceptionHandler
    // Suppress logging of exception by background thread
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = ()
    })
    an[Exception] shouldBe thrownBy {
      noSuchCommand.run(Seq())
    }
    // Restore exception handling
    Thread.setDefaultUncaughtExceptionHandler(defaultHandler)
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

    val copy = new ProducerWithPersistence(RunExternalProcess("cp", InputFileToken("input"), OutputFileToken("output"))(
      inputs = Seq(inputArtifact)
    )
      .outputs("output"),StreamIo, outputArtifact)
    copy.get
    outputFile should exist
    Source.fromFile(outputFile).mkString should equal("Some data\n")

    copy.stepInfo.dependencies.size should equal(1)
    Workflow.upstreamDependencies(copy).size should equal(2)
  }

  it should "pipe stdin to stdout" in {
    val echo = new ExternalProcess("echo", "hello", "world")
    val wc = new ExternalProcess("wc", "-c")
    val result = wc.run(Seq(),stdinput = echo.run(Seq()).stdout)
    IOUtils.readLines(result.stdout()).asScala.head.trim().toInt should equal(11)
  }
}
