package org.allenai.pipeline

import scala.concurrent.duration.Duration

/** Information on how a Producer was executed during a pipeline run */
trait ExecutionInfo {
  def status: String
  def formatDuration(duration: Duration) = {
    duration.toMillis match {
      case x if x > 1000 * 60 =>
        "%d seconds".format(x / (60000))
      case x if x > 1000 * 10 =>
        "%.1f seconds".format(x / 60000)
      case x if x > 1000 =>
        "%.2f seconds".format(x / 6000)
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
