package org.allenai.pipeline

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import java.net.URI

trait PipelineStep {
  def stepInfo: PipelineStepInfo
}

/** @param className Name of implementing class
  * @param classVersion Version ID of implementing class
  * @param parameters Configuration parameters
  * @param dependencies Input steps (name -> step pairs) required to run this step
  * @param description Optional, short description string for this step.
  * @param outputLocation If this step has been Persisted, the URL of the Artifact
  *                       where the data was written.  Specifying a value will not cause a step to
  *                       be persisted.  Rather, when a step is persisted via Producer.persist,
  *                       this field will be populated appropriately.
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

  // Below are convenience methods for building PipelineStepInfo objects by inspecting classes

  // Add parameters and dependencies, inferring the type dynamically
  // Example usage:
  // info =
  //   PipelineStepInfo.basic(this)
  //     .addParameters("seed" -> 117, "upstream" -> inputProducer)
  def addParameters(params: (String, Any)*): PipelineStepInfo = {
    val pipelineSteps = collection.mutable.ListBuffer[(String, PipelineStep)]()
    val otherPars = collection.mutable.ListBuffer[(String, Any)]()
    // parameters of the following types get treated as "dependencies":
    // PipelineStep, Iterable[PipelineStep], Option[PipelineStep]
    params.foreach {
      case (id, p: PipelineStep) => pipelineSteps += ((id, p))
      case (id, it: Iterable[_]) if it.forall(_.isInstanceOf[PipelineStep]) =>
        pipelineSteps ++=
          it.map(_.asInstanceOf[PipelineStep])
          .zipWithIndex
          .map { case (p, i) => (s"${id}_$i", p) }
      case (id, Some(step: PipelineStep)) =>
        pipelineSteps += ((id, step))
      case (id, None) => // no-op: skip None
      case (id, Some(x)) => otherPars += ((id, x))
      case x => otherPars += x
    }
    copy(
      parameters = this.parameters ++ otherPars.map { case (n, v) => (n, String.valueOf(v)) }.toMap,
      dependencies = this.dependencies ++ pipelineSteps.toMap
    )
  }

  // Add parameters and dependencies using the named fields of the given object
  // Example usage:
  // info =
  //  PipelineStepInfo.basic(this)
  //    .addFields(this, "seed", "upstream")
  def addFields(
    obj: Any,
    fieldNames: String*
  ): PipelineStepInfo = {
    val params = for (field <- fieldNames) yield {
      val f = obj.getClass.getDeclaredField(field)
      f.setAccessible(true)
      (field, f.get(obj))
    }
    this.addParameters(params: _*)
  }

  // If the target class is a case class, inspects the class definition
  // to extract the fields named in the constructor
  // Example usage:
  // info =
  //   PipelineStepInfo.basic(this)
  //     .addObject(this)
  def addObject[T <: Product: ClassTag](
    obj: T
  ): PipelineStepInfo = {
    // Scala reflection is not thread-safe in 2.10:
    // http://docs.scala-lang.org/overviews/reflection/thread-safety.html
    synchronized {
      val mirror = scala.reflect.runtime.currentMirror
      val fieldNames = mirror.reflect(obj).symbol.asType.typeSignature.members.collect {
        case m: MethodSymbol if m.isCaseAccessor => m.name.toString
      }.toList
      addFields(obj, fieldNames: _*)
    }
  }

}

object PipelineStepInfo {
  def basic(target: Any) = PipelineStepInfo(target.getClass.getSimpleName)
}

/** Producer implementations that do not need to be executed by PipelineRunner can mix in this
  * convenience trait.  These methods will not be invoked if the output is retrieved by
  * calling Producer.get instead of PipelineRunner.run
  */
trait BasicPipelineStepInfo extends PipelineStep {
  def stepInfo = PipelineStepInfo.basic(this)
}

/** For maximum convenience, a Producer implementation that is a case class
  * can mix in this trait to implement PipelineStep.  The resulting PipelineStepInfo
  * will contain all inputs to the constructor in either the parameters or the dependencies
  */
trait Ai2StepInfo extends Ai2SimpleStepInfo {
  this: Product =>

  override def stepInfo =
    super.stepInfo.addObject(this)
}

/** Any class can mix in this trait to implement PipelineStep.  The resulting PipelineStepInfo
  * will contain only className and classVersion information.  Any parameter or dependency info
  * needs to be added by one of the PipelineStepInfo convenience methods
  */
trait Ai2SimpleStepInfo extends PipelineStep {
  override def stepInfo =
    Ai2CodeInfo(this, classVersion = ("" +: versionHistory).last)
      .copy(description = descriptionOption)

  /** Whenever the logic of this class is updated, the corresponding release number should
    * be added to this list.  The unchangedSince field will be set to the latest version that is
    * still earlier than the version in the jar file.
    */
  def versionHistory: Seq[String] = List()

  def description: String = ""
  protected def descriptionOption =
    if (description != null && description.nonEmpty) Some(description) else None
}
