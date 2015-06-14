package org.allenai.pipeline.spark

import org.allenai.pipeline._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import java.io.{ File, InputStream }

/** Given a collection of S3 paths,
  * parse each line of each file into an object
  * using format given by the implicit StringSerializable
  */
case class ReadObjectRddFromFiles[T: StringSerializable: ClassTag](
    filePaths: Producer[Iterable[String]],
    sparkContext: SparkContext,
    numPartitions: Option[Int] = None
) extends Producer[RDD[T]] with Ai2SimpleStepInfo {
  override def create = {
    DeserializeObject(
      ReadStreamContents(
        ReadInputStreamRddFromFiles(filePaths, numPartitions, sparkContext)
      )
    ).get
  }

  override def stepInfo = {
    val className = scala.reflect.classTag[T].runtimeClass.getSimpleName
    super.stepInfo
      .addParameters(
        "filePaths" -> filePaths,
        "numPartitions" -> numPartitions
      )
      .copy(description = Some(s"Read an RDD of [$className] from lines in the input files"))
  }
}

/** Given a collection of file names,
  * produce an RDD in which each element
  * represents the contents of a file as an InputStream
  */
case class ReadInputStreamRddFromFiles(
  paths: Producer[Iterable[String]],
  numPartitions: Option[Int] = None,
  sparkContext: SparkContext
)
    extends Producer[RDD[() => InputStream]] with Ai2SimpleStepInfo {
  override def create = {
    val pathsRdd =
      numPartitions
        .map(i => sparkContext.parallelize(paths.get.toVector, i))
        .getOrElse(sparkContext.parallelize(paths.get.toVector))
    val contentsRdd = pathsRdd.map {
      path =>
        () => new FileArtifact(new File(path)).read
    }
    contentsRdd
  }

  override def stepInfo =
    super.stepInfo
      .addParameters(
        "paths" -> paths,
        "numPartitions" -> numPartitions
      )

}

