package org.allenai.pipeline

import java.io.File
import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials
import com.typesafe.config.Config

import scala.reflect.ClassTag

abstract class ArtifactFactory(fallback: ArtifactFactory = ArtifactFactory.Empty) {
  def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A]

  /** Create an Artifact at the given path.
    * The type tag determines the type of the Artifact, which may be an abstract type
    */
  def createArtifact[A <: Artifact: ClassTag](path: String): A = {
    val fn = tryCreateArtifact[A] orElse fallback.tryCreateArtifact[A]
    val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    require(fn.isDefinedAt(path), s"Cannot create $clazz from $path")
    fn(path)
  }
}

object ArtifactFactory {
  object Empty extends ArtifactFactory {
    def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] =
      PartialFunction.empty[String, A]
  }
  class FromAbsoluteFileUrl(fallback: ArtifactFactory = Empty) extends ArtifactFactory(fallback) {
    def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[String, A] = {
        case path if c.isAssignableFrom(classOf[FileArtifact])
          && "file" == new URI(path).getScheme =>
          new FileArtifact(new File(new URI(path))).asInstanceOf[A]
        case path if c.isAssignableFrom(classOf[FileArtifact])
          && null == new URI(path).getScheme =>
          new FileArtifact(new File(path)).asInstanceOf[A]
        case path if c.isAssignableFrom(classOf[ZipFileArtifact])
          && "file" == new URI(path).getScheme =>
          new ZipFileArtifact(new File(new URI(path))).asInstanceOf[A]
        case path if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == new URI(path).getScheme =>
          new ZipFileArtifact(new File(path)).asInstanceOf[A]
      }
      fn
    }
  }
  class FromRelativeFilePath(rootDir: File, fallback: ArtifactFactory = Empty) extends ArtifactFactory(fallback) {
    def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[String, A] = {
        case path if c.isAssignableFrom(classOf[FileArtifact])
          && null == new URI(path).getScheme =>
          val file = new File(rootDir, path)
          new FileArtifact(file).asInstanceOf[A]
        case path if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == new URI(path).getScheme =>
          val file = new File(rootDir, path)
          new ZipFileArtifact(file).asInstanceOf[A]
      }
      fn
    }
  }
  class FromAbsoluteS3Url(
      credentials: => BasicAWSCredentials,
      fallback: ArtifactFactory = Empty
  ) extends ArtifactFactory(fallback) {
    def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[String, A] = {
        case urlString if c.isAssignableFrom(classOf[S3FlatArtifact])
          && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
          val url = new URI(urlString)
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3FlatArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
        case urlString if c.isAssignableFrom(classOf[S3ZipArtifact])
          && List("s3", "s3n").contains(new URI(urlString).getScheme) =>
          val url = new URI(urlString)
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3ZipArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
      }
      fn
    }
  }
  class FromRelativeS3Path(
      rootPath: String,
      cfg: => S3Config,
      fallback: ArtifactFactory = Empty
  ) extends ArtifactFactory(fallback) {
    val cleanRoot = rootPath.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
    def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[String, A] = {
        case path if c.isAssignableFrom(classOf[S3FlatArtifact])
          && new URI(path).getScheme == null =>
          val absPath = s"$cleanRoot/$path"
          new S3FlatArtifact(absPath, cfg).asInstanceOf[A]
        case path if c.isAssignableFrom(classOf[S3ZipArtifact])
          && new URI(path).getScheme == null =>
          val absPath = s"$cleanRoot/$path"
          new S3ZipArtifact(absPath, cfg).asInstanceOf[A]
      }
      fn
    }
  }
  def fromUrl(credentials: () => BasicAWSCredentials = S3Config.environmentCredentials): ArtifactFactory = new FromAbsoluteFileUrl(new FromAbsoluteS3Url(credentials()))

  def relativeToUrl[A <: Artifact: ClassTag](
    rootUrl: URI,
    credentials: () => BasicAWSCredentials = S3Config.environmentCredentials,
    fallback: ArtifactFactory = fromUrl()
  ): ArtifactFactory = {
    rootUrl match {
      case url if url.getScheme == "file" || url.getScheme == null =>
        val rootDir = new File(url.getPath)
        new FromRelativeFilePath(rootDir, fallback)
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost, credentials())
        val rootPath = url.getPath
        new FromRelativeS3Path(rootPath, cfg, fallback)
    }
  }
}

