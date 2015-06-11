package org.allenai.pipeline

import scala.concurrent.duration.Duration

/** Information on how a Producer was executed during a pipeline run */
trait ExecutionInfo {
  def status: String
  def formatDuration(duration: Duration) = {
    val seconds = 1000
    val minutes = seconds * 60
    val hours = minutes * 60
    val days = hours * 24
    duration.toMillis match {
      case x if x / days > 1 =>
        "%.2f days".format(x.toDouble / days)
      case x if x / hours > 1 =>
        "%.2f hours".format(x.toDouble / hours)
      case x if x / minutes > 1 =>
        "%.2f minutes".format(x.toDouble / minutes)
      case x if x / seconds > 1 =>
        "%.2f seconds".format(x.toDouble / seconds)
      case x => "%d ms".format(x)
    }
  }
}

/** The Producer was not needed to run the pipeline.
  * (All downstream steps were already persisted)
  */
case object NotRequested extends ExecutionInfo {
  override def status = "Not requested"
}

/** The Producer was executed */
case class Executed(duration: Duration) extends ExecutionInfo {
  override def status = s"Computed in ${formatDuration(duration)}"
}

/* The output was read from previously-computed persisted result */
case class ReadFromDisk(duration: Duration) extends ExecutionInfo {
  override def status = s"Read in ${formatDuration(duration)}"
}

case class ExecutedAndPersisted(duration: Duration) extends ExecutionInfo {
  override def status = s"Computed and stored in ${formatDuration(duration)}"
}

/** The output was an Iterator that was buffered to disk */
case class ExecuteAndBufferStream(duration: Duration) extends ExecutionInfo {
  override def status = s"Computed and buffered in ${formatDuration(duration)}"
}
