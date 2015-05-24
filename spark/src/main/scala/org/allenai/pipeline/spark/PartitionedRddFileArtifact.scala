package org.allenai.pipeline.spark

import org.allenai.pipeline.FileArtifact

import java.io.File
import java.net.URI

class PartitionedRddFileArtifact(rootDir: File) extends PartitionedRddArtifact[FileArtifact] {
  require(
    (rootDir.exists && rootDir.isDirectory)
      || rootDir.mkdirs, s"Unable to find or create directory $rootDir"
  )

  private val successFile = new FileArtifact(new File(rootDir, "_SUCCESS"))

  def makePartitionArtifact: Int => FileArtifact = {
    val root = rootDir
    i: Int => new FileArtifact(new File(root, "part-%05d".format(i)))
  }

  def markWriteSuccess(): Unit = {
    successFile.write(w => w.write(""))
  }

  /** Return true if this data has been written to the persistent store. */
  override def exists: Boolean = successFile.exists

  override def url: URI = rootDir.toURI

  override def getExistingPartitionArtifacts: Iterable[FileArtifact] = {
    rootDir.listFiles.filter(_.getName.startsWith("part-")).map(f => new FileArtifact(f))
  }
}