package org.allenai.pipeline.spark

import org.allenai.common.testkit.ScratchDirectory
import org.allenai.pipeline.{ BasicPipelineStepInfo, ArtifactFactory, FlatArtifact, Producer }

import org.apache.spark.rdd.RDD

import scala.util.Random

import java.io.File

/** Created by rodneykinney on 5/24/15.
  */
class TestPersistRdd extends SparkTest with ScratchDirectory {

  "RddProducer" should "persist correctly" in {
    val outputFile = new File(scratchDir, "test-persist")
    val partitionCount = 10
    val rand = new Random()
    val rows = (0 until 100).map(i => rand.nextDouble)
    val p = Producer.fromMemory(sparkContext.parallelize(rows, partitionCount))
    val pp = {
      import org.allenai.pipeline.IoHelpers._
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

  //  it should "return persisted value" in {
  //    val rand = new Random()
  //
  //    val rdd = sparkContext.parallelize((0 to 10).toVector, 10).map(i => rand.nextDouble())
  //    val p = Producer.fromMemory(rdd)
  //    val first = p.get.collect().toSet
  //    val second = p.get.collect().toSet
  //
  //    first should not equal(second)
  //  }

}
