package org.allenai.pipeline.spark

import org.allenai.common.testkit.UnitSpec

import org.apache.spark.{ SparkConf, SparkContext }

/** Created by rodneykinney on 5/24/15.
  */
class SparkTest extends UnitSpec {
  val sparkContext = SparkTest.sparkContext
}

object SparkTest {
  val sparkContext = {
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "unit-test")
    new SparkContext(conf)
  }
}
