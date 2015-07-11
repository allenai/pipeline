package org.allenai.pipeline

import scala.reflect.ClassTag

import java.io.File
import java.net.URI

/** Creates an Artifact from a URL
  */
trait ArtifactFactory {
  /** @param url The location of the Artifact.  The scheme (protocol) is used to determine the
    *            specific implementation.
    * @tparam A The type of the Artifact to create.  May be an abstract or concrete type
    * @return The artifact
    */
  def createArtifact[A <: Artifact: ClassTag](url: URI): A

  /** If path is an absolute URL, create an Artifact at that location.
    * If it is a relative path, create it relative to the given root URL
    */
  def createArtifact[A <: Artifact: ClassTag](rootUrl: URI, path: String): A = {
    val parsed = new URI(path)
    val url = parsed.getScheme match {
      case null =>
        ArtifactFactory.appendPath(rootUrl, path)
      case _ => parsed
    }
    createArtifact[A](url)
  }
}

object ArtifactFactory {
  def apply(urlHandler: UrlToArtifact, fallbackUrlHandlers: UrlToArtifact*): ArtifactFactory =
    new ArtifactFactory {
      val urlHandlerChain =
        if (fallbackUrlHandlers.isEmpty) {
          urlHandler
        } else {
          UrlToArtifact.chain(urlHandler, fallbackUrlHandlers.head, fallbackUrlHandlers.tail: _*)
        }

      def createArtifact[A <: Artifact: ClassTag](url: URI): A = {
        val fn = urlHandlerChain.urlToArtifact[A]
        val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
        require(fn.isDefinedAt(url), s"Cannot create $clazz from $url")
        fn(url)
      }
    }
  def appendPath(rootUrl: URI, path: String): URI = {
    val fullPath = s"${rootUrl.getPath.reverse.dropWhile(_ == '/').reverse}/${path.dropWhile(_ == '/')}"
    new URI(
      rootUrl.getScheme,
      rootUrl.getHost,
      fullPath,
      rootUrl.getFragment
    )
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
  // Later instances have higher priority and will override earlier instances
  def chain(first: UrlToArtifact, second: UrlToArtifact, others: UrlToArtifact*) =
    new UrlToArtifact {
      override def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
        val all = (List(first, second) ++ others).reverse
        var fn = PartialFunction.empty[URI, A]
        for (o <- all) {
          fn = fn orElse o.urlToArtifact[A]
        }
        fn
      }
    }

  object Empty extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] =
      PartialFunction.empty[URI, A]
  }

}

object CreateCoreArtifacts {
  // Create a FlatArtifact or StructuredArtifact from an absolute file:// URL
  val fromFileUrls: UrlToArtifact = new UrlToArtifact {
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
          && {
            val file = new File(url)
            file.exists && file.isDirectory
          } =>
          new DirectoryArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[DirectoryArtifact])
          && null == url.getScheme
          && {
            val file = new File(url.getPath)
            file.exists && file.isDirectory
          } =>
          new DirectoryArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && "file" == url.getScheme =>
          new ZipFileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          new ZipFileArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[MountedFileArtifact])
          && "mnt" == url.getScheme =>
          new MountedFileArtifact(url.getHost, url.getPath).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[MountedDirectoryArtifact])
          && "mnt" == url.getScheme
          && {
            val file = MountedArtifact.toFile(url.getHost, url.getPath)
            file.exists && file.isDirectory
          } =>
          new MountedDirectoryArtifact(url.getHost, url.getPath).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[MountedZipFileArtifact])
          && "mnt" == url.getScheme =>
          new MountedZipFileArtifact(url.getHost, url.getPath).asInstanceOf[A]
      }
      fn
    }
  }
}

