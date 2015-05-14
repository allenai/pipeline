package org.allenai.pipeline

import java.io.File
import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials

import scala.reflect.ClassTag

object ArtifactFactory {
  def fromAbsoluteUrl[A <: Artifact: ClassTag](
    credentials: () => BasicAWSCredentials = S3Artifact.environmentCredentials
  ): PartialFunction[String, A] = {
    CreateFileArtifact.flatAbsolute[A] orElse
      CreateFileArtifact.structuredAbsolute[A] orElse
      CreateS3Artifact.flatAbsolute[A](credentials()) orElse
      CreateS3Artifact.structuredAbsolute[A](credentials())
  }

  def fromRelativeUrl[A <: Artifact: ClassTag](rootUrl: String, credentials: () => BasicAWSCredentials): PartialFunction[String, A] = {
    new URI(rootUrl) match {
      case url if url.getScheme == "file" || url.getScheme == null =>
        val rootDir = new File(url.getPath)
        CreateFileArtifact.flatRelative[A](rootDir) orElse
          CreateFileArtifact.structuredRelative[A](rootDir)
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost, credentials())
        val base = url.getPath
        CreateS3Artifact.flatRelative[A](cfg, base) orElse
          CreateS3Artifact.structuredRelative[A](cfg, base)
    }
  }
  def apply[A <: Artifact: ClassTag](fn: PartialFunction[String, A])(path: String) = {
    val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    require(fn.isDefinedAt(path), s"Cannot create $clazz from $path")
    fn(path)
  }
}

object CreateS3Artifact {
  def flatAbsolute[A <: Artifact: ClassTag](credentials: => BasicAWSCredentials): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case urlString if c.isAssignableFrom(classOf[S3FlatArtifact])
        && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
        val url = new URI(urlString)
        val bucket = url.getHost
        val path = url.getPath.dropWhile(_ == '/')
        new S3FlatArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
    }
    fn
  }
  def flatRelative[A <: Artifact: ClassTag](cfg: S3Config, root: String): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[S3FlatArtifact])
        && new URI(path).getScheme == null =>
        val cleanRoot = root.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
        val absPath = s"$cleanRoot/$path"
        new S3FlatArtifact(absPath, cfg).asInstanceOf[A]
    }
    fn
  }
  def structuredAbsolute[A <: Artifact: ClassTag](credentials: => BasicAWSCredentials): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case urlString if c.isAssignableFrom(classOf[S3ZipArtifact])
        && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
        val url = new URI(urlString)
        val bucket = url.getHost
        val path = url.getPath.dropWhile(_ == '/')
        new S3ZipArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
    }
    fn
  }
  def structuredRelative[A <: Artifact: ClassTag](cfg: S3Config, root: String): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[S3ZipArtifact])
        && new URI(path).getScheme == null =>
        val cleanRoot = root.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
        val absPath = s"$cleanRoot/$path"
        new S3ZipArtifact(absPath, cfg).asInstanceOf[A]
    }
    fn
  }
}

object CreateFileArtifact {
  def flatAbsolute[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[FileArtifact])
        && "file" == new URI(path).getScheme =>
        new FileArtifact(new File(new URI(path))).asInstanceOf[A]
      case path if c.isAssignableFrom(classOf[FileArtifact])
        && null == new URI(path).getScheme =>
        new FileArtifact(new File(path)).asInstanceOf[A]
    }
    fn
  }
  def flatRelative[A <: Artifact: ClassTag](rootDir: File): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[FileArtifact])
        && null == new URI(path).getScheme =>
        val file = new File(rootDir, path)
        new FileArtifact(file).asInstanceOf[A]
    }
    fn
  }
  def structuredAbsolute[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[ZipFileArtifact])
        && "file" == new URI(path).getScheme =>
        new ZipFileArtifact(new File(new URI(path))).asInstanceOf[A]
      case path if c.isAssignableFrom(classOf[ZipFileArtifact])
        && null == new URI(path).getScheme =>
        new ZipFileArtifact(new File(path)).asInstanceOf[A]
    }
    fn
  }
  def structuredRelative[A <: Artifact: ClassTag](rootDir: File): PartialFunction[String, A] = {
    val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val fn: PartialFunction[String, A] = {
      case path if c.isAssignableFrom(classOf[ZipFileArtifact])
        && null == new URI(path).getScheme =>
        val file = new File(rootDir, path)
        new ZipFileArtifact(file).asInstanceOf[A]
    }
    fn
  }
}

