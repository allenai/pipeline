package org.allenai.pipeline.spark

import org.allenai.common.testkit.ScratchDirectory
import org.allenai.pipeline._

import scala.util.Random

import java.io.File

/** Created by rodneykinney on 5/24/15.
  */
class TestPersistRdd extends SparkTest with ScratchDirectory {

  import org.allenai.pipeline.IoHelpers._

  "RddProducer" should "persist correctly" in {
    val outputFile = new File(scratchDir, "test-persist")
    val partitionCount = 10
    val rand = new Random()
    val rows = (0 until 100).map(i => rand.nextDouble)
    val p = Producer.fromMemory(sparkContext.parallelize(rows, partitionCount))
    val pp = {
      val af = ArtifactFactory(CreateRddArtifacts.fromFileUrls)
      val outputArtifact = af.createArtifact[PartitionedRddArtifact](outputFile.toURI)
      p.persisted(new PartitionedRddIo[Double](sparkContext), outputArtifact)
    }
    val result = pp.get

    result.collect().toSet should equal(rows.toSet)

    pp.artifact.exists should be(true)

    // Should create one file for each partition, plus one for the _SUCCESS file
    outputFile.listFiles.size should be(partitionCount + 1)
  }

  it should "not cache persisted RDDs in memory" in {
    val rand = new Random()
    val doubles = (0 until 1000).map(i => rand.nextDouble)
    val dir = new File(scratchDir, "persistCache")
    val numbersFile = new File(dir, "numbers.txt")
    LineCollectionIo.text[Double].write(doubles, new FileArtifact(numbersFile))

    val pipeline = SparkPipeline(sparkContext, dir.toURI)

    val numbersProducer = {
      val read = Producer.fromMemory(sparkContext.textFile(numbersFile.getPath).map(_.toDouble))
      pipeline.persistRdd(read)
    }

    val first = numbersProducer.get.collect().toSet
    // If the RDD is recomputed, deleting this file will cause it to fail
    numbersFile.delete()
    // Should succeed because RDD is read from persisted location
    val second = numbersProducer.get.collect().toSet

    first should equal(second)
  }

}
