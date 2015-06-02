package org.allenai.pipeline.spark

import org.allenai.pipeline.FileArtifact

import java.io.File
import java.net.URI

class PartitionedRddFileArtifact(
    rootDir: File,
    prefix: String = "part-",
    maxPartitions: Int = 100000
) extends PartitionedRddArtifact {
  require(
    (rootDir.exists && rootDir.isDirectory)
      || rootDir.mkdirs, s"Unable to find or create directory $rootDir"
  )
  private val digits = (math.log(maxPartitions - 1) / math.log(10)).toInt + 1

  def makePartitionArtifact: Int => FileArtifact = {
    val root = rootDir
    val prefix = this.prefix
    val digits = this.digits
    i: Int => new FileArtifact(new File(root, s"$prefix%0${digits}d".format(i)))
  }

  private def successFile = new FileArtifact(new File(rootDir, "_SUCCESS"))

  def saveWasSuccessful(): Unit =
    successFile.write(w => w.write(""))

  /** Return true if this data has been written to the persistent store. */
  override def exists: Boolean = successFile.exists

  override def url: URI = rootDir.toURI

  override def getExistingPartitions: Iterable[Int] = {
    rootDir.list()
      .filter(_.startsWith(prefix))
      .map(_.substring(prefix.length).toInt)
      .toList
  }
}