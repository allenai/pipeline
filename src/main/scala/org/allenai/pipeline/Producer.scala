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
      copy(cachingEnabled = () => true)
    }
  }

  /** Default caching policy is set by the implementing class but can be overridden dynamically.  */
  def disableCaching: Producer[T] = {
    if (cachingEnabled) {
      copy(cachingEnabled = () => false)
    } else this
  }

  def copy[NT](create: () => NT = self.create _,
               signature: () => Signature = self.signature _,
               codeInfo: () => CodeInfo = self.codeInfo _,
               cachingEnabled: () => Boolean = self.cachingEnabled _) = {
    val _create = create
    val _signature = signature
    val _codeInfo = codeInfo
    val _cachingEnabled = cachingEnabled
    self match {
      case p: HasPath => new Producer[NT] with HasPath {
        override def create = _create()

        override def signature = _signature()

        override def codeInfo = _codeInfo()

        override def cachingEnabled = _cachingEnabled()

        override def path = p.path
      }
      case _ => new Producer[NT] {
        override def create = _create()

        override def signature = _signature()

        override def codeInfo = _codeInfo()

        override def cachingEnabled = _cachingEnabled()
      }

    }
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
  self =>
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

  def asArtifact = copy(create = () => {
    if (!artifact.exists)
      io.write(step.get, artifact)
    artifact
  })

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
    val p1 = p.copy(create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1"))

    val p2 = p.copy(create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2"))
    Some((p1, p2))
  }
}

object Producer3 {
  def unapply[T1, T2, T3](p: Producer[(T1, T2, T3)]): Option[(Producer[T1], Producer[T2], Producer[T3])] = {
    val p1 = p.copy(create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1"))
    val p2 = p.copy(create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2"))
    val p3 = p.copy(create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3"))
    Some((p1, p2, p3))
  }
}

object Producer4 {
  def unapply[T1, T2, T3, T4](p: Producer[(T1, T2, T3, T4)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4])] = {
    val p1 = p.copy(create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1"))
    val p2 = p.copy(create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2"))
    val p3 = p.copy(create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3"))
    val p4 = p.copy(create = () => p.get._4,
      signature = () => p.signature.copy(name = s"${p.signature.name}_4"))
    Some((p1, p2, p3, p4))
  }
}

object Producer5 {
  def unapply[T1, T2, T3, T4, T5](p: Producer[(T1, T2, T3, T4, T5)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4], Producer[T5])] = {
    val p1 = p.copy(create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1"))
    val p2 = p.copy(create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2"))
    val p3 = p.copy(create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3"))
    val p4 = p.copy(create = () => p.get._4,
      signature = () => p.signature.copy(name = s"${p.signature.name}_4"))
    val p5 = p.copy(create = () => p.get._5,
      signature = () => p.signature.copy(name = s"${p.signature.name}_5"))
    Some((p1, p2, p3, p4, p5))
  }
}
