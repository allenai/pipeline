package org.allenai.pipeline

import org.allenai.common.Logging

/** An individual step in a data processing pipeline
  * A lazily evaluated calculation, with support for in-memory caching and persistence
  */
trait Producer[T] extends Logging {
  outer =>
  /** Return the computed value
    * @return
    */
  def get: T

  /** Persist the result of this step
    * Once computed, write the result to the given artifact
    * If the artifact we are using for persistence exists, return the deserialized object rather than recomputing it
    * @param io
    * @param artifactSource
    * @tparam A
    * @return
    */
  def persisted[A <: Artifact](io: ArtifactIO[T, A], artifactSource: => A): PersistedProducer[T, A] = new PersistedNoncachedProducer(this, io, artifactSource)

  /** Default caching policy is set by the implementing class but can be overridden dynamically
    */
  def enableCaching: Producer[T] = new CachedProducer[T] {
    def create = outer.get
  }

  /** Default caching policy is set by the implementing class but can be overridden dynamically
    */
  def disableCaching: Producer[T] = this
}

/** A Pipeline step with caching
  * A reference to the output of the step will be stored in memory
  * Using a CachedStep is never appropriate if the output is an Iterator object
  * @tparam T
  */
trait CachedProducer[T] extends Producer[T] {
  private lazy val cachedValue = {
    val result = create
    require(!result.isInstanceOf[Iterator[Any]], "Attempt to cache an instance of Iterator.  Please disable caching when using Iterator return types")
    result
  }

  def create: T

  override def get = cachedValue

  /** Persist the result of this step
    * Caching will apply only to the deserialized object
    * @param io
    * @param artifactSource
    * @tparam A
    * @return
    */
  override def persisted[A <: Artifact](io: ArtifactIO[T, A], artifactSource: => A): PersistedProducer[T, A] = new PersistedCachedProducer(this, io, artifactSource)

  override def enableCaching = this
  override def disableCaching = new Producer[T] {
    def get = create
  }
}

trait PersistedProducer[T, A <: Artifact] extends Producer[T] {
  def artifact: A
  protected def writeToArtifact(): Unit
  def asArtifact = new Producer[A] {
    def get = {
      if (!artifact.exists)
        writeToArtifact
      artifact
    }
  }
}

class PersistedNoncachedProducer[T, A <: Artifact](step: Producer[T], io: ArtifactIO[T, A], artifactSource: => A) extends PersistedProducer[T, A] {
  lazy val artifact = artifactSource

  def get = {
    if (!artifact.exists) {
      val result = step.get
      logger.debug(s"Writing to $artifact using $io")
      io.write(result, artifact)
    }
    logger.debug(s"Reading from $artifact using $io")
    io.read(artifact)
  }

  protected def writeToArtifact = io.write(step.get, artifact)

}

class PersistedCachedProducer[T, A <: Artifact](step: CachedProducer[T], io: ArtifactIO[T, A], artifactSource: => A) extends PersistedProducer[T, A] {
  lazy val artifact = artifactSource

  def get = cachedValue

  private lazy val cachedValue = {
    if (artifact.exists) {
      logger.debug(s"Reading from $artifact using $io")
      io.read(artifact)
    } else {
      val result = step.create
      require(!result.isInstanceOf[Iterator[Any]], s"Attempt to cache an instance of Iterator in $step.  Please disable caching when using Iterator return types")
      logger.debug(s"Writing to $artifact using $io")
      io.write(result, artifact)
      result
    }
  }

  protected def writeToArtifact = io.write(step.get, artifact)
  override def enableCaching: Producer[T] = this
  override def disableCaching: Producer[T] = new PersistedNoncachedProducer(step.disableCaching, io, artifactSource)
}

//
// Allow un-zipping of Producer instances
// e.g.:
//   val tupleProducer: Producer[List[Int], List[String]]
//   Producer2(test, train) = tupleProducer
//
object Producer2 {
  def unapply[T1, T2](p: Producer[(T1, T2)]): Option[(Producer[T1], Producer[T2])] = {
    val p1 = new Producer[T1] {
      def get = p.get._1
    }
    val p2 = new Producer[T2] {
      def get = p.get._2
    }
    Some((p1, p2))
  }
}
object Producer3 {
  def unapply[T1, T2, T3](p: Producer[(T1, T2, T3)]): Option[(Producer[T1], Producer[T2], Producer[T3])] = {
    val p1 = new Producer[T1] {
      def get = p.get._1
    }
    val p2 = new Producer[T2] {
      def get = p.get._2
    }
    val p3 = new Producer[T3] {
      def get = p.get._3
    }
    Some((p1, p2, p3))
  }
}
object Producer4 {
  def unapply[T1, T2, T3, T4](p: Producer[(T1, T2, T3, T4)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4])] = {
    val p1 = new Producer[T1] {
      def get = p.get._1
    }
    val p2 = new Producer[T2] {
      def get = p.get._2
    }
    val p3 = new Producer[T3] {
      def get = p.get._3
    }
    val p4 = new Producer[T4] {
      def get = p.get._4
    }
    Some((p1, p2, p3, p4))
  }
}
object Producer5 {
  def unapply[T1, T2, T3, T4, T5](p: Producer[(T1, T2, T3, T4, T5)]): Option[(Producer[T1], Producer[T2], Producer[T3], Producer[T4], Producer[T5])] = {
    val p1 = new Producer[T1] {
      def get = p.get._1
    }
    val p2 = new Producer[T2] {
      def get = p.get._2
    }
    val p3 = new Producer[T3] {
      def get = p.get._3
    }
    val p4 = new Producer[T4] {
      def get = p.get._4
    }
    val p5 = new Producer[T5] {
      def get = p.get._5
    }
    Some((p1, p2, p3, p4, p5))
  }
}
