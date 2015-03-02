package org.allenai.pipeline

import org.allenai.common.Logging

import java.net.URI

/** An individual step in a data processing pipeline.
  * A lazily evaluated calculation, with support for in-memory caching and persistence.
  *
  * @tparam  T  the type of data being produced
  */
trait Producer[T] extends Logging with CachingEnabled with PipelineRunnerSupport {
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
    * If the artifact we are using for persistence exists,
    * return the deserialized object rather than recomputing it.
    *
    * @tparam  A  the type of artifact being writen to (i.e. directory, file)
    * @param  io  the serialization for data of type T
    * @param  artifactSource  creation of the artifact to be written
    */
  def persisted[A <: Artifact](
    io: ArtifactIo[T, A],
    artifactSource: => A
  ): PersistedProducer[T, A] =
    new PersistedProducer(this, io, artifactSource)

  /** Default caching policy is set by the implementing class but can be overridden dynamically.
    *
    * When caching is enabled, an in-memory reference is stored to the output object so
    * subsequent calls to .get do not re-process.
    */
  def enableCaching: Producer[T] = {
    if (cachingEnabled) {
      this
    } else {
      copy(cachingEnabled = () => true)
    }
  }

  /** Default caching policy is set by the implementing class but can be overridden dynamically. */
  def disableCaching: Producer[T] = {
    if (cachingEnabled) {
      copy(cachingEnabled = () => false)
    } else {
      this
    }
  }

  def copy[T2](
    create: () => T2 = self.create _,
    signature: () => Signature = self.signature _,
    codeInfo: () => CodeInfo = self.codeInfo _,
    cachingEnabled: () => Boolean = self.cachingEnabled _,
    description: () => Option[String] = self.description _
  ): Producer[T2] = {
    val _create = create
    val _signature = signature
    val _codeInfo = codeInfo
    val _cachingEnabled = cachingEnabled
    val _description = description
    new Producer[T2] {
      override def create: T2 = _create()

      override def signature: Signature = _signature()

      override def codeInfo: CodeInfo = _codeInfo()

      override def cachingEnabled: Boolean = _cachingEnabled()

      override def outputLocation: Option[URI] = self.outputLocation
    }
  }

  override def outputLocation: Option[URI] = None
}

object Producer {
  /** A Pipeline step wrapper for in-memory data. */
  def fromMemory[T](data: T): Producer[T] = new Producer[T] with UnknownCodeInfo {
    override def create: T = data

    override def signature: Signature = Signature(
      data.getClass.getName,
      data.hashCode.toHexString
    )

    override def outputLocation: Option[URI] = None
  }
}

/** This information is used by PipelineRunner to construct and visualize the DAG for a pipeline */
trait PipelineRunnerSupport extends HasCodeInfo {
  /** Represents a digest of the logic that will uniquely determine the output of this Producer
    * Includes the inputs (other Producer instances feeding into this one)
    * and parameters (other static configuration)
    * and code version (a release id that should only change when the internal class logic changes)
    */
  def signature: Signature

  /** If this Producer has been Persisted, this field will contain the URL of the Artifact
    * where the data was written.  This field should not be specified in the Producer
    * implementation class. Specifying a value will not cause the Producer to be persisted.
    * Rather, when a PersistedProducer is created, it will populate this field appropriately.
    */
  def outputLocation: Option[URI]

  /** An optional, short description string for this step. */
  def description: Option[String] = None
}

/** Producer implementations that do not need to be executed by PipelineRunner can mix in this
  * convenience trait.  These methods will not be invoked if the output is retrieved by
  * calling Producer.get instead of PipelineRunner.run
  */
trait NoPipelineRunnerSupport extends PipelineRunnerSupport {
  override def codeInfo: CodeInfo = ???

  override def signature: Signature = ???

  override def outputLocation: Option[URI] = ???
}

trait CachingEnabled {
  def cachingEnabled: Boolean = true
}

trait CachingDisabled extends CachingEnabled {
  override def cachingEnabled: Boolean = false
}

class PersistedProducer[T, A <: Artifact](step: Producer[T], io: ArtifactIo[T, A],
    artifactSource: => A) extends Producer[T] {
  self =>
  lazy val artifact = artifactSource

  def create: T = {
    if (!artifact.exists) {
      val result = step.get
      logger.debug(s"Writing to $artifact using $io")
      io.write(result, artifact)
    }
    logger.debug(s"Reading from $artifact using $io")
    io.read(artifact)
  }

  def asArtifact: Producer[A] = copy(create = () => {
    if (!artifact.exists) {
      io.write(step.get, artifact)
    }
    artifact
  })

  override def signature: Signature = step.signature

  override def codeInfo: CodeInfo = step.codeInfo

  override def outputLocation: Option[URI] = Some(artifact.url)

  override def description = Option(step).flatMap(_.description)
}

//
// Allow un-zipping of Producer instances
// e.g.:
//   val tupleProducer: Producer[List[Int], List[String]]
//   val Producer2(intList, stringList) = tupleProducer
//
object Producer2 {
  def unapply[T1, T2](p: Producer[(T1, T2)]): Option[(Producer[T1], Producer[T2])] = {
    val p1 = p.copy(
      create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1")
    )

    val p2 = p.copy(
      create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2")
    )
    Some((p1, p2))
  }
}

object Producer3 {
  def unapply[T1, T2, T3](
    p: Producer[(T1, T2, T3)]
  ): Option[(Producer[T1], Producer[T2], Producer[T3])] = {
    val p1 = p.copy(
      create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1")
    )
    val p2 = p.copy(
      create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2")
    )
    val p3 = p.copy(
      create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3")
    )
    Some((p1, p2, p3))
  }
}

object Producer4 {
  private type P[T] = Producer[T] // Reduce line length

  def unapply[T1, T2, T3, T4](p: P[(T1, T2, T3, T4)]): Option[(P[T1], P[T2], P[T3], P[T4])] = {
    val p1 = p.copy(
      create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1")
    )
    val p2 = p.copy(
      create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2")
    )
    val p3 = p.copy(
      create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3")
    )
    val p4 = p.copy(
      create = () => p.get._4,
      signature = () => p.signature.copy(name = s"${p.signature.name}_4")
    )
    Some((p1, p2, p3, p4))
  }
}

object Producer5 {
  private type P[T] = Producer[T] // Reduce line length

  def unapply[T1, T2, T3, T4, T5](
    p: P[(T1, T2, T3, T4, T5)]
  ): Option[(P[T1], P[T2], P[T3], P[T4], P[T5])] = {
    val p1 = p.copy(
      create = () => p.get._1,
      signature = () => p.signature.copy(name = s"${p.signature.name}_1")
    )
    val p2 = p.copy(
      create = () => p.get._2,
      signature = () => p.signature.copy(name = s"${p.signature.name}_2")
    )
    val p3 = p.copy(
      create = () => p.get._3,
      signature = () => p.signature.copy(name = s"${p.signature.name}_3")
    )
    val p4 = p.copy(
      create = () => p.get._4,
      signature = () => p.signature.copy(name = s"${p.signature.name}_4")
    )
    val p5 = p.copy(
      create = () => p.get._5,
      signature = () => p.signature.copy(name = s"${p.signature.name}_5")
    )
    Some((p1, p2, p3, p4, p5))
  }
}
