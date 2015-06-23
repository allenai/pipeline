package org.allenai.pipeline

import java.io.File
import java.net.URI

/** A file resource that is mounted on the local filesystem from a share
  * The mount point is represented by an environment variable
  *
  * The URL for a MountedFileArtifact is of the form:
  * mnt://MOUNT_POINT_ENV/<path>
  *
  * The representing URL is therefore the same for all users, even though
  * the path on each user's local filesystem may be different
  */
class MountedFileArtifact(mountPointEnvVar: String, relativePath: String)
    extends FileArtifact(MountedArtifact.toFile(mountPointEnvVar, relativePath)) {
  override def url = new URI("mnt", mountPointEnvVar, relativePath, null)
}

class MountedZipFileArtifact(mountPointEnvVar: String, relativePath: String)
    extends ZipFileArtifact(MountedArtifact.toFile(mountPointEnvVar, relativePath)) {
  override def url = new URI("mnt", mountPointEnvVar, relativePath, null)
}

class MountedDirectoryArtifact(mountPointEnvVar: String, relativePath: String)
    extends DirectoryArtifact(MountedArtifact.toFile(mountPointEnvVar, relativePath)) {
  override def url = new URI("mnt", mountPointEnvVar, relativePath, null)
}

object MountedArtifact {
  def toFile(mountPointEnvVar: String, relativePath: String) = {
    val dirPath = System.getenv(mountPointEnvVar)
    require(dirPath != null, s"Environment variable $mountPointEnvVar not set")
    val dir = new File(dirPath)
    new File(dir, relativePath)
  }
}
