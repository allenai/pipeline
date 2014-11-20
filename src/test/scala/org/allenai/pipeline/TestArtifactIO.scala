package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.{ ScratchDirectory, UnitSpec }
import spray.json.DefaultJsonProtocol._
import org.allenai.pipeline.IoHelpers._

import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
// scalastyle:off magic.number
class TestArtifactIo extends UnitSpec with ScratchDirectory {

  val rand = new Random

  "JSONFormat" should "persist primitive types in Tuples" in {
    val data = for (i <- (0 until 1000)) yield {
      (rand.nextInt(i + 1), rand.nextDouble)
    }

    val io = LineCollectionIo.json[(Int, Double)]
    val file = new File(scratchDir, "ioTest.json")
    val artifact = new FileArtifact(file)
    io.write(data, artifact)
    val persistedData = io.read(artifact)
    persistedData should equal(data)

    val iteratorData = LineIteratorIo.json[(Int, Double)].read(artifact)
    iteratorData.toList should equal(data)
  }

  "TSVFormat" should "persist case classes" in {
    case class XYZ(x: Int, y: String, z: Double, w: String)
    implicit val yFormat = columnFormat4(XYZ)

    val file = new File(scratchDir, "tsvTest.txt")
    val io = LineCollectionIo.text[XYZ]
    val artifact = new FileArtifact(file)
    val data = (1 to 100) map (i => XYZ(rand.nextInt(i), rand.nextInt(100).toString, rand.nextDouble, i.toString))
    io.write(data, artifact)
    val persistedData = io.read(artifact)
    persistedData should equal(data)

    val iteratorData = LineIteratorIo.text[XYZ].read(artifact)
    iteratorData.toList should equal(data)
  }

  "TSVFormat" should "persist Tuples" in {
    implicit val zFormat = tuple3ColumnFormat[Int, String, Double]()
    val zIO = LineCollectionIo.text[Tuple3[Int, String, Double]]
    val file = new File(scratchDir, "tsvTupleTest.txt")
    val zArtifact = new FileArtifact(file)
    val z = (1 to 100) map (i => (rand.nextInt(i), rand.nextInt(100).toString, rand.nextDouble))
    zIO.write(z, zArtifact)
    val zOut = zIO.read(zArtifact)

    zOut should equal(z)
  }
}
