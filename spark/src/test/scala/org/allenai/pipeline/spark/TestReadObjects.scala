package org.allenai.pipeline.spark

import org.allenai.pipeline.IoHelpers._

import java.io.File

/**
 * Created by rodneykinney on 5/24/15.
 */
class TestReadObjects extends SparkTest {
  "ReadObjectRdd" should "read from files" in {
    val paths = Read.Collection.fromText[String](new File("src/test/resources/paths.txt"))
    val objects = ReadObjectRddFromFiles[Int](paths, sparkContext)
    val result = objects.get.collect().toSet

    result should equal((1 to 16).toSet)
  }

}
