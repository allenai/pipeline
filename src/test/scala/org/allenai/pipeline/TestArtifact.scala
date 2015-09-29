package org.allenai.pipeline

import java.nio.file.Files

import scala.io.Source

import java.io.{ InputStream, File }

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.scalatest._

import scala.collection.mutable.ListBuffer
import scala.util.Random

class TestArtifact extends UnitSpec with ScratchDirectory {
  private val fileArtifactFactories =
    Seq[File => FileArtifact](new FileArtifact(_), new CompressedFileArtifact(_))

  "FileArtifact" should "read/write" in {
    fileArtifactFactories foreach { makeFileArtifact =>
      val rand = new Random()

      val file = new File(scratchDir, "flatFile.txt")
      val a = makeFileArtifact(file)
      val buff = new ListBuffer[String]
      a.write { w =>
        for (i <- (0 until 100)) {
          val s = s"$i\t${rand.nextDouble}"
          w.println(s)
          buff += s
        }
      }
      val lines = Source.fromInputStream(a.read).getLines.toList

      lines should equal(buff.toList)
      file.delete()
    }
  }

  it should "write valid utf-8" in {
    fileArtifactFactories foreach { makeFileArtifact =>
      val input = "The term \ud835\udc43(\ud835\udc43\ud835\udc5d) in the equation below"
      val file = new File(scratchDir, "flatFile.txt")
      val a = makeFileArtifact(file)
      a.write(_.write(input))
      val output = Source.fromInputStream(a.read).mkString
      input should equal(output)
    }
  }

  "ZipFileArtifact" should "read/write" in {
    val rand = new Random()

    val file = new File(scratchDir, "archive.zip")
    val z = new ZipFileArtifact(file)
    val numbers = new ListBuffer[Int]
    val letters = new ListBuffer[String]
    z.write { w =>
      val alphabet = "abcdefghijklmnopqrstuvwxyz"
      w.writeEntry("letters") { entry =>
        for (i <- 0 until 100) {
          val l = alphabet(rand.nextInt(alphabet.size)).toString
          letters += l
          entry.println(l)
        }
      }
      w.writeEntry("numbers") { entry =>
        for (i <- 0 until 100) {
          val num = rand.nextInt(alphabet.size)
          numbers += num
          entry.println(s"$num")
        }
      }
    }

    def readLines(is: InputStream) = Source.fromInputStream(is).getLines.toList
    def readInts(is: InputStream) = Source.fromInputStream(is).getLines.map(_.toInt).toList
    val reader = z.reader
    val x = (readInts(reader.read("numbers")), readLines(reader.read("letters")))

    for ((name, is) <- reader.readAll) name match {
      case "numbers" => readInts(is) should equal(numbers)
      case "letters" => readLines(is) should equal(letters)
    }

    numbers should equal(x._1)
    letters should equal(x._2)
    file.delete()
  }

  "DirectoryArtifact" should "support nested entries" in {
    def entryNames(s: StructuredArtifact) = s.reader.readAll.map(_._1).toSet

    def countFiles(f: File): Int =
      f.listFiles.map(f => if (f.isDirectory) countFiles(f) else 1).sum

    val originalFile = new File("src/test/scala")
    val original = new DirectoryArtifact(originalFile)

    entryNames(original) should have size (countFiles(originalFile))

    val copy = new ZipFileArtifact(Files.createTempFile(null, null).toFile)
    original.copyTo(copy)
    entryNames(copy) should equal(entryNames(original))

    val copiedBackFile = Files.createTempDirectory(null).toFile
    val copiedBack = new DirectoryArtifact(copiedBackFile)
    copy.copyTo(copiedBack)
    entryNames(copiedBack) should equal(entryNames(original))

    countFiles(originalFile) should equal(countFiles(copiedBackFile))
  }
}
