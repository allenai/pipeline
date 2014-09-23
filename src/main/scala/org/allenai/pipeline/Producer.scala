package org.allenai.pipeline

import org.allenai.common.Logging

/** An individual step in a data processing pipeline.
  * A lazily evaluated calculation, with support for in-memory caching and persistence.
  */
trait Producer[T] extends Logging with CachingEnabled with HasSignature with HasCodeInfo {
  self =>
  /** Return the computed value. */
  def create: T

  def get: T = {
    if (cachingEnabled && cachedValue.isDefined) cachedValue.get else create
  }

  private lazy val cachedValue: Option[T] = {
    val result = create
    if (result.isInstanceOf[Iterator[_]]) None else Some(result)
  }

  /** Persist the result of this step.
    * Once computed, write the result to the given artifact.
    * If the artifact we are using for persistence exists, return the deserialized object rather than recomputing it.
    */
  def persisted[A <: Artifact](io: ArtifactIo[T, A], artifactSource: => A): PersistedProducer[T, A] = new PersistedProducer(this, io, artifactSource)

  /** Default caching policy is set by the implementing class but can be overridden dynamically.
    */
  def enableCaching: Producer[T] = {
    if (cachingEnabled) {
      this
    } else {
      new Producer[T] with CachingEnabled {
        def create = self.create

        def signature = self.signature

        def codeInfo = self.codeInfo
      }
    }
  }

  /** Default caching policy is set by the implementing class but can be overridden dynamically.  */
  def disableCaching: Producer[T] = {
    if (cachingEnabled) {
      new Producer[T] with CachingDisabled {
        def create = self.create

        def signature = self.signature

        def codeInfo = self.codeInfo
      }
    } else this
  }
}

trait CachingEnabled {
  def cachingEnabled = true
}

trait CachingDisabled extends CachingEnabled {
  override def cachingEnabled = false
}

class PersistedProducer[T, A <: Artifact](step: Producer[T], io: ArtifactIo[T, A],
                                          artifactSource: => A) extends Producer[T] with HasPath {
  lazy val artifact = artifactSource

  override def path = artifact.path

  def create = {
    if (!artifact.exists) {
      val result = step.get
      logger.debug(s"Writing to $artifact using $io")
      io.write(result, artifact)
    }
    logger.debug(s"Reading from $artifact using $io")
    io.read(artifact)
  }

  def asArtifact = new Producer[A] {
    def create = {
      if (!artifact.exists)
        io.write(step.get, artifact)
      artifact
    }

    def signature = step.signature

    def codeInfo = step.codeInfo

  }

  def signature = step.signature

  def codeInfo = step.codeInfo

}

//
// Allow un-zipping of Producer instances
// e.g.:
//   val tupleProducer: Producer[List[Int], List[String]]
//   val Producer2(intList, stringList) = tupleProducer
//
object Producer2 {
  def unapply[T1, T2](p: Producer[(T1, T2)]): Option[(Producer[T1], Producer[T2])] = {
    val p1 = new Producer[T1] {
      def create = p.get._1

      def signature = p.signature.copy(name = p.signature.name + "_1")

      def codeInfo = p.codeInfo

    }
    val p2 = new Producer[T2] {
      def create = p.get._2

      def signature = p.signature.copy(name = p.signature.name + "_2")

      def codeInfo = p.codeInfo
    }
    Some((p1, p2))
  }
}

object Producer3 {
  def unapply[T1, T2, T3](p: Producer[(T1, T2, T3)]): Option[(Producer[T1], Producer[T2], Producer[T3])] = {
    val p1 = new Producer[T1] {
      def create = p.get._1

      def signature = p.signature.copy(name = p.signature.name + "_1")

      def codeInfo = p.codeInfo
    }
    val p2 = new Producer[T2] {
      def create = p.get._2

      def signature = p.signature.copy(name = p.signature.name + "_2")

      def codeInfo = p.codeInfo
    }
    val p3 = new Producer[T3] {
      def create = p.get._3

      def signature = p.signature.copy(name = p.signature.name + "_3")

      def codeInfo = p.codeInfo
    }
    Some((p1, p2, p3))
  }
}

object Producer4 {
  def unapply[T1, T2, T3, T4](p: Producer[(T1, T2, T3, T4)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4])] = {
    val p1 = new Producer[T1] {
      def create = p.get._1

      def signature = p.signature.copy(name = p.signature.name + "_1")

      def codeInfo = p.codeInfo
    }
    val p2 = new Producer[T2] {
      def create = p.get._2

      def signature = p.signature.copy(name = p.signature.name + "_2")

      def codeInfo = p.codeInfo
    }
    val p3 = new Producer[T3] {
      def create = p.get._3

      def signature = p.signature.copy(name = p.signature.name + "_3")

      def codeInfo = p.codeInfo
    }
    val p4 = new Producer[T4] {
      def create = p.get._4

      def signature = p.signature.copy(name = p.signature.name + "_4")

      def codeInfo = p.codeInfo
    }
    Some((p1, p2, p3, p4))
  }
}

object Producer5 {
  def unapply[T1, T2, T3, T4, T5](p: Producer[(T1, T2, T3, T4, T5)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4], Producer[T5])] = {
    val p1 = new Producer[T1] {
      def create = p.get._1

      def signature = p.signature.copy(name = p.signature.name + "_1")

      def codeInfo = p.codeInfo
    }
    val p2 = new Producer[T2] {
      def create = p.get._2

      def signature = p.signature.copy(name = p.signature.name + "_2")

      def codeInfo = p.codeInfo
    }
    val p3 = new Producer[T3] {
      def create = p.get._3

      def signature = p.signature.copy(name = p.signature.name + "_3")

      def codeInfo = p.codeInfo
    }
    val p4 = new Producer[T4] {
      def create = p.get._4

      def signature = p.signature.copy(name = p.signature.name + "_4")

      def codeInfo = p.codeInfo
    }
    val p5 = new Producer[T5] {
      def create = p.get._5

      def signature = p.signature.copy(name = p.signature.name + "_5")

      def codeInfo = p.codeInfo
    }
    Some((p1, p2, p3, p4, p5))
  }
}
