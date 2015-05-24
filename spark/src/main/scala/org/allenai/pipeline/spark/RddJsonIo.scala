package org.allenai.pipeline.spark

import org.allenai.pipeline.{LineIteratorIo, ArtifactIo, FlatArtifact}

import org.apache.spark.SparkContext
import spray.json.JsonFormat

import scala.reflect.ClassTag

/**
 * Created by rodneykinney on 5/24/15.
 */
class RddJsonIo[T : JsonFormat : ClassTag](sc: SparkContext)
  extends PartitionedRddIo[T, FlatArtifact](sc) {
  override def makePartitionIo: () => ArtifactIo[Iterator[T], FlatArtifact] =
    () => LineIteratorIo.json[T]
}
