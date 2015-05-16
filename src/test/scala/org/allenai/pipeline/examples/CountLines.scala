package org.allenai.pipeline.examples

import org.allenai.pipeline.{Producer, Ai2StepInfo}

/**
 * Created by rodneykinney on 5/16/15.
 */
case class CountLines(lines: Producer[Iterable[String]])
extends Producer[Int] with Ai2StepInfo{
  override protected def create: Int = lines.get.size
}
