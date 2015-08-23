package org.allenai.pipeline.examples

import org.allenai.pipeline.{ Ai2StepInfo, Producer }

/** Created by rodneykinney on 5/16/15.
  */
case class CountWords(lines: Producer[Iterable[String]])
    extends Producer[Map[String, Int]] with Ai2StepInfo {
  override def create = {
    val words = for {
      line <- lines.get
      word <- line.split("\\s+")
    } yield word
    words.groupBy(w => w).mapValues(_.size)
  }
}
