package org.allenai.pipeline.examples

import org.allenai.pipeline._

import scala.util.Random

import java.io.InputStream

/** Classes for use in model-training pipeline
  */
case class TrainModel(trainingData: Producer[Iterable[TrainingPoint]])
    extends Producer[TrainedModel] with Ai2StepInfo {
  def create: TrainedModel = {
    val dataRows = trainingData.get
    train(dataRows) // Run training algorithm on training data
  }

  def train(data: Iterable[TrainingPoint]): TrainedModel =
    TrainedModel(s"Trained model with ${data.size} rows")

  override val description = "Train teh model.  Teh."
}

case class TrainedModel(info: String)

case class JoinAndSplitData(
    features: Producer[Iterable[Array[Double]]],
    labels: Producer[Iterable[Boolean]],
    testSizeRatio: Double
) extends Producer[(Iterable[TrainingPoint], Iterable[TrainingPoint])] with Ai2StepInfo {
  def create = {
    val data =
      for ((label, features) <- labels.get.zip(features.get)) yield TrainingPoint(label, features)
    val testSize = math.round(testSizeRatio * data.size).toInt
    (data.drop(testSize), data.take(testSize))
  }

  override val description = "Join and split data."
}

object ParseDocumentsFromXML extends Deserializer[Iterator[ParsedDocument], StructuredArtifact]
    with Ai2SimpleStepInfo {
  def read(a: StructuredArtifact): Iterator[ParsedDocument] = {
    for ((id, is) <- a.reader.readAll) yield parse(id, is)
  }

  def parse(id: String, is: InputStream): ParsedDocument = ParsedDocument(id)

  override def toString: String = this.getClass.getSimpleName
}

case class ParsedDocument(info: String)

case class FeaturizeDocuments(documents: Producer[Iterator[ParsedDocument]])
    extends Producer[Iterable[Array[Double]]] with Ai2StepInfo {
  def create: Iterable[Array[Double]] = {
    val features = for (doc <- documents.get) yield {
      val rand = new Random
      Array.fill(8)(rand.nextDouble) // scalastyle:ignore
    }
    features.toList
  }
}

case class TrainingPoint(label: Boolean, features: Array[Double])

case class PR(precision: Double, recall: Double, threshold: Double)

case class MeasureModel(
    val model: Producer[TrainedModel],
    val testData: Producer[Iterable[TrainingPoint]]
) extends Producer[Iterable[PR]] with Ai2StepInfo {
  def create = {
    model.get
    // Just generate some dummy data
    val rand = new Random
    import scala.math.exp
    var a = 0.0
    var b = 0.0
    val prScan = for (i <- (0 until testData.get.size)) yield {
      val r = PR(exp(-a), 1 - exp(-b), exp(-b))
      a += rand.nextDouble * .03
      b += rand.nextDouble * .03
      r
    }
    prScan
  }

  override val description = "Measure the model."
}

