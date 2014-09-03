package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec
import spray.json.DefaultJsonProtocol

import scala.util.Random

/** Created by rodneykinney on 8/19/14.
  */
class TestArtifactIO extends UnitSpec {

  import DefaultJsonProtocol._
  import TsvFormats._

  val rand = new Random

  "JSONFormat" should "persist primitive types in Tuples" in {
    val data = for (i <- (0 until 1000)) yield {
      (rand.nextInt(i + 1), rand.nextDouble)
    }

    val io = new JsonCollectionIO[(Int, Double)]
    val file = new File("ioTest.txt")
    val artifact = new FileArtifact(file)
    io.write(data, artifact)
    val persistedData = io.read(artifact)
    persistedData should equal(data)

    val iteratorData = new JsonIteratorIO[(Int, Double)].read(artifact)
    iteratorData.toList should equal(data)

    file.delete()
  }

  "TSVFormat" should "persist case classes" in {
    case class XYZ(x: Int, y: String, z: Double, w: String)
    implicit val yFormat = tsvFormat4(XYZ)

    val file = new File("tsvTest.txt")
    val io = new TsvCollectionIO[XYZ]
    val artifact = new FileArtifact(file)
    val data = (1 to 100) map (i => XYZ(rand.nextInt(i), rand.nextInt(100).toString, rand.nextDouble, i.toString))
    io.write(data, artifact)
    val persistedData = io.read(artifact)
    persistedData should equal(data)

    val iteratorData = new TsvIteratorIO[XYZ].read(artifact)
    iteratorData.toList should equal(data)

    file.delete()
  }

  "TSVFormat" should "persist Tuples" in {
    implicit val zFormat = tsvTuple3Format[Int, String, Double]()
    val zIO = new TsvCollectionIO[Tuple3[Int, String, Double]]
    val file = new File("tsvTupleTest.txt")
    val zArtifact = new FileArtifact(file)
    val z = (1 to 100) map (i => (rand.nextInt(i), rand.nextInt(100).toString, rand.nextDouble))
    zIO.write(z, zArtifact)
    val zOut = zIO.read(zArtifact)

    zOut should equal(z)

    file.delete()
  }
}
