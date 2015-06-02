package org.allenai.pipeline

import scala.io.Source

import java.io.{ InputStream, File }

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import org.scalatest._

import scala.collection.mutable.ListBuffer
import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
class TestArtifact extends UnitSpec with ScratchDirectory {
  "FileArtifact" should "read/write" in {
    val rand = new Random()

    val file = new File(scratchDir, "flatFile.txt")
    val a = new FileArtifact(file)
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

  it should "write valid utf-8" in {
    val input = "The term \ud835\udc43(\ud835\udc43\ud835\udc5d) in the equation below"
    val file = new File(scratchDir, "flatFile.txt")
    val a = new FileArtifact(file)
    a.write(_.write(input))
    val output = Source.fromInputStream(a.read).mkString
    input should equal(output)
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
}
