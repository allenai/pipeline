package org.allenai.pipeline

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import java.net.URI

trait PipelineStep {
  def stepInfo: PipelineStepInfo
}

/**
 *
 * @param className Name of implementing class
 * @param classVersion Version ID of implementing class
 * @param parameters Configuration parameters
 * @param dependencies Input steps
 * @param description Optional, short description string for this step.
 * @param outputLocation If this step has been Persisted, the URL of the Artifact
 *                       where the data was written.  Specifying a value will not cause a step to be persisted.
 *                       Rather, when a step is persisted via Producer.persist, this field will be populated appropriately.
 */
case class PipelineStepInfo(
  className: String,
  classVersion: String = "",
  srcUrl: Option[URI] = None,
  binaryUrl: Option[URI] = None,
  parameters: Map[String, String] = Map(),
  dependencies: Map[String, PipelineStep] = Map(),
  description: Option[String] = None,
  outputLocation: Option[URI] = None
) {
  /** Represents a digest of the logic that will uniquely determine the output of this Producer
    * Includes the inputs (other Producer instances feeding into this one)
    * and parameters (other static configuration)
    * and code version (a release id that should only change when the internal class logic changes)
    */
  val signature = new Signature(
    name = className,
    unchangedSinceVersion = classVersion,
    dependencies = dependencies,
    parameters = parameters
  )
}

object PipelineStepInfo {
  def basic(target: Any) = PipelineStepInfo(target.getClass.getSimpleName)

  def apply(className: String, classVersion: String, params: (String, Any)*): PipelineStepInfo = {
    val (deps, other) = params.partition(_._2.isInstanceOf[PipelineStep])
    val (containersWithDeps, pars) = other.partition(t => t._2.isInstanceOf[Iterable[_]] &&
        t._2.asInstanceOf[Iterable[_]].forall(_.isInstanceOf[PipelineStep]))
    val containedDeps = for {
      (id, depList: Iterable[_]) <- containersWithDeps
      (d, i) <- depList.zipWithIndex
    } yield (s"${id}_$i", d)
    PipelineStepInfo(
      className = className,
      classVersion = classVersion,
      dependencies = (deps ++ containedDeps).map {
        case (n, p: PipelineStep) => (n, p)
      }.toMap,
      parameters = pars.map { case (n, value) => (n, String.valueOf(value)) }.toMap
    )
  }

  def fromFields(
    base: Any,
    fieldNames: String*
  ): PipelineStepInfo = {
    val params = for (field <- fieldNames) yield {
      val f = base.getClass.getDeclaredField(field)
      f.setAccessible(true)
      (field, f.get(base))
    }
    val info = Ai2CodeInfo(base)
    apply(info.className, info.classVersion, params: _*)
  }

  // The most magical of the Signature factory methods
  // If the target class is a case class, inspects the class definition
  // to extract the fields named in the constructor
  def fromObject[T <: Product : ClassTag](obj: T): PipelineStepInfo = {
    // Scala reflection is not thread-safe in 2.10:
    // http://docs.scala-lang.org/overviews/reflection/thread-safety.html
    synchronized {
      val mirror = scala.reflect.runtime.currentMirror
      val fieldNames = mirror.reflect(obj).symbol.asType.typeSignature.members.collect {
        case m: MethodSymbol if m.isCaseAccessor => m.name.toString
      }.toList
      fromFields(obj, fieldNames: _*)
    }
  }

}

/** Producer implementations that do not need to be executed by PipelineRunner can mix in this
  * convenience trait.  These methods will not be invoked if the output is retrieved by
  * calling Producer.get instead of PipelineRunner.run
  */
trait BasicPipelineStepInfo extends PipelineStep {
  def stepInfo = PipelineStepInfo.basic(this)
}

/** For convenience, case classes can mix in this single trait to implement PipelineStep
  */
trait Ai2StepInfo extends PipelineStep {
  this: Product =>

  override def stepInfo = PipelineStepInfo.fromObject(this)
      .copy(description = if (description.nonEmpty) Some(description) else None)

  def description: String = ""
}

trait Ai2SimpleStepInfo extends PipelineStep {
  override def stepInfo = Ai2CodeInfo(this)
}
