package org.allenai.pipeline

import java.io.{ File, FileInputStream, FileOutputStream, InputStream }
import java.util.zip.{ ZipEntry, ZipFile, ZipOutputStream }

import scala.collection.JavaConverters._

/** Flat file
  * @param file
  */
class FileArtifact(val file: File) extends FlatArtifact {
  def exists = file.exists

  // Caller is responsible for closing the InputStream.
  // Unfortunately necessary to support streaming
  def read: InputStream = new FileInputStream(file)

  // Note:  The write operation is atomic.  The file is only created if the write operation completes successfully
  def write[T](writer: ArtifactStreamWriter => T): T = {
    val tmpFile = File.createTempFile(file.getName, "tmp", file.getParentFile)
    val fileOut = new FileOutputStream(tmpFile)
    val out = new ArtifactStreamWriter(fileOut)
    val result = writer(out)
    fileOut.close()
    require(tmpFile.renameTo(file), s"Unable to create $file")
    result
  }

  override def toString = s"FileArtifact[$file]"
}

/** Directory of files
  * @param dir
  */
class DirectoryArtifact(val dir: File) extends StructuredArtifact {
  private val parentDir = dir.getAbsoluteFile.getParentFile
  require((parentDir.exists && parentDir.isDirectory) || parentDir.mkdirs, s"Unable to find or create directory $dir")
  def exists = dir.exists && dir.isDirectory
  def reader: StructuredArtifactReader = new StructuredArtifactReader {
    require(exists, s"Attempt to read for non-existent directory $dir")
    /** Throw exception if file does not exist */
    def read(entryName: String): InputStream = new FileInputStream(new File(dir, entryName))

    /** Read only read plain files (no recursive directory search) */
    def readAll: Iterator[(String, InputStream)] = dir.listFiles.iterator.filterNot(_.isDirectory).map(f => (f.getName, new FileInputStream(f)))
  }

  /** Writing to a directory is atomic, like other artifacts
    * However, if the directory already exists, it will be renamed as a backup
    * @param writer
    * @tparam T
    * @return
    */
  def write[T](writer: StructuredArtifactWriter => T): T = {
    backupExistingDirectory()
    val tmpDir = createTempDirectory
    val dirWriter = new StructuredArtifactWriter {
      def writeEntry[T](name: String)(writer: ArtifactStreamWriter => T): T = {
        val out = new FileOutputStream(new File(tmpDir, name))
        val result = writer(new ArtifactStreamWriter(out))
        out.close()
        result
      }
    }
    val result = writer(dirWriter)
    require(tmpDir.renameTo(dir), s"Unable to create directory $dir")
    result
  }
  private def createTempDirectory = {
    val f = File.createTempFile(dir.getName, ".tmp", parentDir)
    f.delete()
    f.mkdir
    new File(f.getPath)
  }
  // If the directory we are writing to already exists, rename it to a backup name
  // This way we ensure that the directory contains only what was written to it
  private def backupExistingDirectory() {
    if (dir.exists) {
      val backupFile = Stream.from(1).map(i => new File(parentDir, s"${dir.getName}-$i")).dropWhile(_.exists).head
      require(dir.renameTo(backupFile), s"Unable to back up existing copy of $dir")
    }
  }
  override def toString = s"DirectoryArtifact[$dir]"
}

/** Zip file
  * @param file
  */
class ZipFileArtifact(val file: File) extends StructuredArtifact {
  def exists = file.exists

  def reader: StructuredArtifactReader = {
    require(exists, s"Cannot read from non-existent file $file")
    new ZipFileReader(file)
  }

  // Atomic write operation
  def write[T](writer: StructuredArtifactWriter => T): T = {
    val w = new ZipFileWriter(file)
    val result = writer(w)
    w.close()
    result
  }

  override def toString = s"ZipFileArtifact[$file]"

  class ZipFileReader(file: File) extends StructuredArtifactReader {
    private val zipFile = new ZipFile(file)

    // Will throw exception if non-existent entry name is given
    def read(entryName: String): InputStream = zipFile.getInputStream(zipFile.getEntry(entryName))

    // Read all entries in order
    def readAll: Iterator[(String, InputStream)] = {
      for (entry <- zipFile.entries.asScala) yield (entry.getName, zipFile.getInputStream(entry))
    }
  }

  class ZipFileWriter(file: File) extends StructuredArtifactWriter {
    private val tmpFile = File.createTempFile(file.getName, "tmp", file.getParentFile)
    private val zipOut = new ZipOutputStream(new FileOutputStream(tmpFile))
    private val out = new ArtifactStreamWriter(zipOut)

    // Atomic write operation
    def writeEntry[T](name: String)(writer: ArtifactStreamWriter => T): T = {
      zipOut.putNextEntry(new ZipEntry(name))
      val result = writer(out)
      zipOut.closeEntry()
      result
    }

    private[ZipFileArtifact] def close() = {
      zipOut.close
      require(tmpFile.renameTo(file), s"Unable to create $file")
    }
  }

}
