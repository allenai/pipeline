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

  // Add parameters and dependencies, inferring the type dynamically
  def addParameters(params: (String, Any)*): PipelineStepInfo = {
    val (deps, other) = params.partition(_._2.isInstanceOf[PipelineStep])
    val (containersWithDeps, pars) = other.partition(t => t._2.isInstanceOf[Iterable[_]] &&
        t._2.asInstanceOf[Iterable[_]].forall(_.isInstanceOf[PipelineStep]))
    val containedDeps = for {
      (id, depList: Iterable[_]) <- containersWithDeps
      (d, i) <- depList.zipWithIndex
    } yield (s"${id}_$i", d)
    val depMap = (deps ++ containedDeps).map {
      case (n, p: PipelineStep) => (n, p)
    }.toMap
    val paramMap = pars.map { case (n, value) => (n, String.valueOf(value))}.toMap
    copy(
      parameters = this.parameters ++ paramMap,
      dependencies = this.dependencies ++ depMap)
  }


  // Add parameters and dependencies using the named fields of the given object
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

  // The most magical of the factory methods
  // If the target class is a case class, inspects the class definition
  // to extract the fields named in the constructor
  // and adds them to the parameters and dependencies
  def addObject[T <: Product : ClassTag](
    obj: T): PipelineStepInfo = {
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

/** For convenience, case classes can mix in this single trait to implement PipelineStep
  */
trait Ai2StepInfo extends Ai2SimpleStepInfo {
  this: Product =>

  override def stepInfo =
    super.stepInfo.addObject(this)
}

trait Ai2SimpleStepInfo extends PipelineStep {
  override def stepInfo = Ai2CodeInfo(this, classVersion)
      .copy(description = descriptionOption)

  /** Whenever the logic of this class is updated, the corresponding release number should
    * be added to this list.  The unchangedSince field will be set to the latest version that is
    * still earlier than the version in the jar file.
    */
  val versionHistory: Seq[String] = List()

  def description: String = ""
  private def descriptionOption =
    if (description.nonEmpty) Some(description) else None

  private val classVersion = ("" +: versionHistory).last

}
