package org.allenai.pipeline

import java.io.File
import java.net.URI

object ArtifactFactory {
  def fromAbsoluteUrl[A <: Artifact]: PartialFunction[(String, Class[A]), A] = {
    CreateFileArtifact.flatAbsolute[A] orElse
      CreateFileArtifact.structuredAbsolute[A] orElse
      CreateS3Artifact.flatAbsolute[A] orElse
      CreateS3Artifact.structuredAbsolute[A]
  }

  def fromRelativeUrl[A <: Artifact](rootUrl: String): PartialFunction[(String, Class[A]), A] = {
    new URI(rootUrl) match {
      case url if url.getScheme == "file" || url.getScheme == null =>
        val rootDir = new File(url.getPath)
        CreateFileArtifact.flatRelative[A](rootDir) orElse
          CreateFileArtifact.structuredRelative[A](rootDir)
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost)
        val base = url.getPath
        CreateS3Artifact.flatRelative[A](cfg, base) orElse
          CreateS3Artifact.structuredRelative[A](cfg, base)
    }
  }
}

object CreateS3Artifact {
  def flatAbsolute[A <: Artifact]: PartialFunction[(String, Class[A]), A] = {
    case (urlString, c) if c.isAssignableFrom(classOf[S3FlatArtifact])
      && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
      val url = new URI(urlString)
      val bucket = url.getHost
      val path = url.getPath.dropWhile(_ == '/')
      new S3FlatArtifact(path, S3Config(bucket)).asInstanceOf[A]
  }
  def flatRelative[A <: Artifact](cfg: S3Config, root: String): PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[S3FlatArtifact])
      && new URI(path).getScheme == null =>
      val cleanRoot = root.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
      val absPath = s"$cleanRoot/$path"
      new S3FlatArtifact(absPath, cfg).asInstanceOf[A]
  }
  def structuredAbsolute[A <: Artifact]: PartialFunction[(String, Class[A]), A] = {
    case (urlString, c) if c.isAssignableFrom(classOf[S3ZipArtifact])
      && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
      val url = new URI(urlString)
      val bucket = url.getHost
      val path = url.getPath.dropWhile(_ == '/')
      new S3ZipArtifact(path, S3Config(bucket)).asInstanceOf[A]
  }
  def structuredRelative[A <: Artifact](cfg: S3Config, root: String): PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[S3ZipArtifact])
      && new URI(path).getScheme == null =>
      val cleanRoot = root.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
      val absPath = s"$cleanRoot/$path"
      new S3ZipArtifact(absPath, cfg).asInstanceOf[A]
  }
}

object CreateFileArtifact {
  def flatAbsolute[A <: Artifact]: PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[FileArtifact])
      && "file" == new URI(path).getScheme =>
      new FileArtifact(new File(new URI(path))).asInstanceOf[A]
  }
  def flatRelative[A <: Artifact](rootDir: File): PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[FileArtifact])
      && null == new URI(path).getScheme =>
      val file = new File(rootDir, path)
      new FileArtifact(file).asInstanceOf[A]
  }
  def structuredAbsolute[A <: Artifact]: PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[ZipFileArtifact])
      && "file" == new URI(path).getScheme =>
      new ZipFileArtifact(new File(new URI(path))).asInstanceOf[A]
  }
  def structuredRelative[A <: Artifact](rootDir: File): PartialFunction[(String, Class[A]), A] = {
    case (path, c) if c.isAssignableFrom(classOf[ZipFileArtifact])
      && null == new URI(path).getScheme =>
      val file = new File(rootDir, path)
      new ZipFileArtifact(file).asInstanceOf[A]
  }
}

