package org.allenai.pipeline.examples

import org.allenai.pipeline.{ Ai2StepInfo, Producer }

/** Created by rodneykinney on 5/16/15.
  */
case class CountLines(lines: Producer[Iterable[String]], countBlanks: Boolean = true) extends Producer[Int] with Ai2StepInfo {
  override protected def create: Int =
    if (countBlanks)
      lines.get.size
    else
      lines.get.filter(_.trim.length > 0).size

  override def versionHistory = List(
    "v1.1" // Count whitespace-only lines as blank
  )
}
