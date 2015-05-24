package org.allenai.pipeline.spark

import org.allenai.pipeline.{StringSerializable, ArtifactIo, FlatArtifact, LineIteratorIo}

import org.apache.spark.SparkContext
import spray.json.JsonFormat

import scala.reflect.ClassTag

/**
  * Created by rodneykinney on 5/24/15.
  */
class RddStringSerializableIo[T : StringSerializable : ClassTag](sc: SparkContext)
  extends PartitionedRddIo[T, FlatArtifact](sc) {
     override def makePartitionIo: () => ArtifactIo[Iterator[T], FlatArtifact] =
       () => LineIteratorIo.text[T]
   }
