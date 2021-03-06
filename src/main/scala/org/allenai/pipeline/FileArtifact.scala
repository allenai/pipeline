package org.allenai.pipeline

import org.allenai.common.Resource

import org.apache.commons.compress.compressors.bzip2.{ BZip2CompressorOutputStream, BZip2CompressorInputStream }

import java.io._
import java.net.URI
import java.util.zip.{ ZipEntry, ZipFile, ZipOutputStream }

import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._

/** Flat file.  */
class FileArtifact(val file: File) extends FlatArtifact {
  protected val parentDir = {
    val f = file.getCanonicalFile.getParentFile
    FileUtils.forceMkdir(f)
    f
  }

  override def exists: Boolean = file.exists

  override def url: URI = file.getCanonicalFile.toURI

  // Caller is responsible for closing the InputStream.
  // Unfortunately necessary to support streaming
  def read: InputStream = new FileInputStream(file)

  // Note:  The write operation is atomic.  The file is only created if the write operation
  // completes successfully
  def write[T](writer: ArtifactStreamWriter => T): T = {
    val tmpFile = File.createTempFile(file.getName, ".tmp", parentDir)
    tmpFile.deleteOnExit()
    val result = Resource.using(new BufferedOutputStream(new FileOutputStream(tmpFile))) {
      fileOut => writer(new ArtifactStreamWriter(fileOut))
    }
    require(tmpFile.renameTo(file), s"Unable to create $file")
    result
  }

  override def toString: String = s"FileArtifact[${file.getCanonicalPath}]"
}

/** Compressed flat file.  */
class CompressedFileArtifact(file: File) extends FileArtifact(file) {
  override def read: InputStream = new BZip2CompressorInputStream(new FileInputStream(file))

  override def write[T](writer: ArtifactStreamWriter => T): T = {
    val tmpFile = File.createTempFile(file.getName, ".tmp", parentDir)
    tmpFile.deleteOnExit()
    val result = Resource.using(new BZip2CompressorOutputStream(new FileOutputStream(tmpFile))) {
      fileOut => writer(new ArtifactStreamWriter(fileOut))
    }
    require(tmpFile.renameTo(file), s"Unable to create $file")
    result
  }

  override def toString: String = s"CompressedFileArtifact[${file.getCanonicalPath}]"
}

/** Directory of files.  */
class DirectoryArtifact(val dir: File) extends StructuredArtifact {

  import org.allenai.pipeline.StructuredArtifact._

  override def url: URI = dir.getCanonicalFile.toURI

  private val parentDir = {
    val f = dir.getCanonicalFile.getParentFile
    require(
      (f.exists && f.isDirectory) || f.mkdirs,
      s"Unable to find or create directory $dir"
    )
    f
  }

  override def exists: Boolean = dir.exists && dir.isDirectory

  override def reader: Reader = new Reader {
    require(exists, s"Attempt to read for non-existent directory $dir")

    /** Throw exception if file does not exist. */
    def read(entryName: String): InputStream =
      new BufferedInputStream(new FileInputStream(new File(dir, entryName)))

    /** Read all plain files recursively */
    def readAll: Iterator[(String, InputStream)] = {
      def readDir(prefix: String, dir: File): Iterable[(String, InputStream)] = {
        dir.listFiles.flatMap { f =>
          if (f.isDirectory) {
            readDir(s"${prefix}${f.getName}/", f)
          } else {
            List((s"${prefix}${f.getName}", new BufferedInputStream(new FileInputStream(f))))
          }
        }
      }
      readDir("", dir).iterator
    }
  }

  /** Writing to a directory is atomic, like other artifacts.
    * However, if the directory already exists, it will be renamed as a backup.
    */
  override def write[T](writer: Writer => T): T = {
    if (dir.exists) {
      FileUtils.cleanDirectory(dir)
    }
    val tmpDir = createTempDirectory
    val dirWriter = new Writer {
      def writeEntry[T2](name: String)(writer: ArtifactStreamWriter => T2): T2 = {
        val outFile = new File(tmpDir, name)
        require(outFile.getParentFile == null || outFile.getParentFile.exists || outFile.getParentFile.mkdirs(), s"Cannot create file $outFile")
        val out = new BufferedOutputStream(new FileOutputStream(outFile))
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
    val tmpDir = new File(f.getPath)
    scala.sys.addShutdownHook(FileUtils.deleteDirectory(tmpDir))
    tmpDir
  }

  override def toString: String = s"DirectoryArtifact[$dir]"
}

/** Zip file.  */
class ZipFileArtifact(val file: File) extends StructuredArtifact {

  import org.allenai.pipeline.StructuredArtifact._

  override def exists: Boolean = file.exists

  override def url: URI = file.getCanonicalFile.toURI

  override def reader: Reader = {
    require(exists, s"Cannot read from non-existent file $file")
    new ZipFileReader(file)
  }

  // Atomic write operation
  override def write[T](writer: Writer => T): T = {
    val w = new ZipFileWriter(file)
    val result = writer(w)
    w.close()
    result
  }

  override def toString: String = s"ZipFileArtifact[$file]"

  class ZipFileReader(file: File) extends Reader {
    private val zipFile = new ZipFile(file)

    // Will throw exception if non-existent entry name is given
    def read(entryName: String): InputStream = zipFile.getInputStream(zipFile.getEntry(entryName))

    // Read all entries in order
    def readAll: Iterator[(String, InputStream)] = {
      for (entry <- zipFile.entries.asScala) yield (entry.getName, zipFile.getInputStream(entry))
    }
  }

  class ZipFileWriter(file: File) extends Writer {
    private val parentDir = file.getCanonicalFile.getParentFile
    require(
      (parentDir.exists && parentDir.isDirectory) || parentDir.mkdirs,
      s"Unable to find or create directory $parentDir"
    )
    private val tmpFile = File.createTempFile(file.getName, ".tmp", parentDir)
    tmpFile.deleteOnExit()
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
      zipOut.close()
      require(tmpFile.renameTo(file), s"Unable to create $file")
    }
  }
}
