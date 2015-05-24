package org.allenai.pipeline.spark

import org.allenai.common.testkit.ScratchDirectory
import org.allenai.pipeline.{ArtifactFactory, FlatArtifact, Producer}

import scala.util.Random

import java.io.File

/**
 * Created by rodneykinney on 5/24/15.
 */
class TestPersistRdd extends SparkTest with ScratchDirectory {

  "RddProducer" should "persist correctly" in {
    val rand = new Random()
    val rows = (0 until 100).map(i => rand.nextDouble)
    val p = Producer.fromMemory(sparkContext.parallelize(rows, 10))
    val pp = {
      import org.allenai.pipeline.IoHelpers._
      val af = ArtifactFactory(RddArtifacts.handleFileUrls)
      val outputFile = new File(scratchDir,"test-persist")
      val outputArtifact = af.createArtifact[PartitionedRddArtifact[FlatArtifact]](outputFile.toURI)
      p.persisted(new PartitionedRddIo[Double](sparkContext), outputArtifact)
    }
    val result = pp.get

    result.collect().toSet should equal(rows.toSet)
  }

}
