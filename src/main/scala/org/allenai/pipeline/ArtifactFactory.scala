package org.allenai.pipeline

import java.io.File
import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials

import scala.reflect.ClassTag

/** Creates an Artifact from a String
  */
trait ArtifactFactory {
  /** @param path The path of the Artifact.  May be a relative path or absolute URL
    * @tparam A The type of the Artifact to create
    * @return The artifact
    */
  def createArtifact[A <: Artifact: ClassTag](path: String): A
}

object ArtifactFactory {
  def apply(fromUrl: UrlToArtifact, fallbacks: UrlToArtifact*): ArtifactFactory =
    new ArtifactFactory {
      val combinedFromUrl =
        if (fallbacks.size == 0)
          fromUrl
        else
          UrlToArtifact.chain(fromUrl, fallbacks.head, fallbacks.tail: _*)
      def createArtifact[A <: Artifact: ClassTag](path: String): A = {
        var fn = combinedFromUrl.urlToArtifact[A]
        val url = new URI(path)
        val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
        require(fn.isDefinedAt(url), s"Cannot create $clazz from $path")
        fn(url)
      }
    }
}

/** Supports creation of a particular type of Artifact from a URL.
  * Allows chaining together of different implementations that recognize different input URLs
  * and support creation of different Artifact types
  */
trait UrlToArtifact {
  /** Return a PartialFunction indicating whether the given Artifact type can be created from an input URL
    * @tparam A The Artifact type to be created
    * @return A PartialFunction where isDefined will return true if an Artifact of type A can
    *         be created from the given URL
    */
  def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A]
}

object UrlToArtifact {
  // Chain together a series of UrlToArtifact instances
  // The result will be a UrlToArtifact that supports creation of the union of Artifact types and input URLs
  // that are supported by the individual inputs
  def chain(first: UrlToArtifact, second: UrlToArtifact, others: UrlToArtifact*) =
    new UrlToArtifact {
      override def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
        var fn = first.urlToArtifact[A] orElse second.urlToArtifact[A]
        for (o <- others) {
          fn = fn orElse o.urlToArtifact[A]
        }
        fn
      }
    }
  object Empty extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] =
      PartialFunction.empty[URI, A]
  }

  // Create a FlatArtifact or StructuredArtifact from an absolute file:// URL
  object absoluteFile extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && "file" == url.getScheme =>
          new FileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && null == url.getScheme =>
          new FileArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[DirectoryArtifact])
          && "file" == url.getScheme
          && new File(url).exists
          && new File(url).isDirectory =>
          new DirectoryArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[DirectoryArtifact])
          && null == url.getScheme
          && new File(url.getPath).exists
          && new File(url.getPath).isDirectory =>
          new DirectoryArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && "file" == url.getScheme =>
          new ZipFileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          new ZipFileArtifact(new File(url.getPath)).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtifact from a path relative to the input rootDir
  def relativeFile(rootDir: File) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && null == url.getScheme =>
          val file = new File(rootDir, url.getPath)
          new FileArtifact(file).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          val file = new File(rootDir, url.getPath)
          new ZipFileArtifact(file).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtifact from an absolute s3:// URL
  def absoluteS3(credentials: => BasicAWSCredentials) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[S3FlatArtifact])
          && List("s3", "s3n").contains(url.getScheme) =>
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3FlatArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[S3ZipArtifact])
          && List("s3", "s3n").contains(url.getScheme) =>
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3ZipArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtifact from a path relative to the input s3:// URL
  def relativeS3(
    cfg: => S3Config,
    rootPath: String
  ) = new UrlToArtifact {
    val cleanRoot = rootPath.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[S3FlatArtifact])
          && url.getScheme == null =>
          val absPath = s"$cleanRoot/${url.getPath}"
          new S3FlatArtifact(absPath, cfg).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[S3ZipArtifact])
          && url.getScheme == null =>
          val absPath = s"$cleanRoot/${url.getPath}"
          new S3ZipArtifact(absPath, cfg).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtfact from an input file:// or s3:// URL
  def absoluteUrl(credentials: => BasicAWSCredentials = S3Config.environmentCredentials): UrlToArtifact =
    UrlToArtifact.chain(absoluteFile, absoluteS3(credentials))

  // Create a FlatArtifact or StructuredArtifact from a path relative to the input file:// or s3:// URL
  def relativeToUrl[A <: Artifact: ClassTag](
    rootUrl: URI,
    credentials: => BasicAWSCredentials = S3Config.environmentCredentials
  ): UrlToArtifact = {
    rootUrl match {
      case url if url.getScheme == "file" || url.getScheme == null =>
        val rootDir = new File(url.getPath)
        UrlToArtifact.chain(relativeFile(rootDir), absoluteFile)
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost, credentials)
        val rootPath = url.getPath
        UrlToArtifact.chain(relativeS3(cfg, rootPath), absoluteS3(credentials))
    }
  }
}

