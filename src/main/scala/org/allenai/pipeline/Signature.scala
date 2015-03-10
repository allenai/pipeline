package org.allenai.pipeline

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Acts as an identifier for a Producer instance.  Represents the version of the implementation
  * class, the inputs, and the static configuration.  The PipelineRunner class uses a Producer's
  * Signature to  determine the path to the output data, so two Producers with the same signature
  * must always produce identical output.
  * @param name Human-readable name for the calculation done by a Producer.  Usually the class
  *         name, typically a verb
  * @param unchangedSinceVersion The latest version number at which the logic for this class
  *                          changed. Default is "0", meaning all release builds of this
  *                          class have equivalent logic
  * @param dependencies The inputs to the Producer
  * @param parameters Static configuration for the Producer.  Default is to use .toString for
  *               constructor parameters that are not Producer instances.  If some
  *               parameters are non-primitive types, those types should have .toString
  *               methods that are consistent with .equals.
  */
case class Signature(
    name: String,
    unchangedSinceVersion: String,
    dependencies: Map[String, PipelineRunnerSupport],
    parameters: Map[String, String]
) {
  def id: String = {
    val hashString = this.toJson.compactPrint
    val hashCodeLong = hashString.foldLeft(0L) { (hash, char) => hash * 31 + char }
    hashCodeLong.toHexString
  }

  def infoString: String = this.toJson.prettyPrint

}

object Signature {

  implicit val jsonWriter: JsonWriter[Signature] = new JsonWriter[Signature] {
    private val NAME = "name"
    private val CODE_VERSION_ID = "codeVersionId"
    private val DEPENDENCIES = "dependencies"
    private val PARAMETERS = "parameters"

    def write(s: Signature): JsValue = {
      // Sort keys in dependencies and parameters so that json format is identical for equal objects
      val deps = s.dependencies.toList.map(t => (t._1, jsonWriter.write(t._2.signature))).
        sortBy(_._1).toJson
      val params = s.parameters.toList.sortBy(_._1).toJson
      JsObject(
        (NAME, JsString(s.name)),
        (CODE_VERSION_ID, JsString(s.unchangedSinceVersion)),
        (DEPENDENCIES, deps),
        (PARAMETERS, params)
      )
    }
  }

  def apply(name: String, unchangedSinceVersion: String, params: (String, Any)*): Signature = {
    val pipelineRunners = collection.mutable.ListBuffer[(String, PipelineRunnerSupport)]()
    val otherParams = collection.mutable.ListBuffer[(String, Any)]()
    // parameters of the following types get treated as "dependencies":
    // PipelineRunnerSupport, Iterable[PipelineRunnerSupport], Option[PipelineRunnerSupport]
    params.foreach {
      case (id, p: PipelineRunnerSupport) => pipelineRunners += ((id, p))
      case (id, it: Iterable[PipelineRunnerSupport]) =>
        pipelineRunners ++= it.zipWithIndex.map { case (p, i) => (s"${id}_$i", p)}
      case (id, opt: Some[PipelineRunnerSupport]) =>
        pipelineRunners += ((id, opt.get))
      case (id, None) => // no-op: skip None
      case x => otherParams += x
    }
    Signature(
      name = name,
      unchangedSinceVersion = unchangedSinceVersion,
      dependencies = pipelineRunners.toMap,
      parameters = otherParams.map { case (n, value) => (n, String.valueOf(value)) }.toMap
    )
  }

  def fromFields(
    base: HasCodeInfo,
    fieldNames: String*
  ): Signature =
    fromInfoAndFields(base.codeInfo, base, fieldNames: _*)

  def fromInfoAndFields(
    info: CodeInfo,
    base: Any,
    fieldNames: String*
  ): Signature = {
    val params = for (field <- fieldNames) yield {
      val f = base.getClass.getDeclaredField(field)
      f.setAccessible(true)
      (field, f.get(base))
    }
    apply(info.className, info.unchangedSince, params: _*)
  }

  // The most magical of the Signature factory methods
  // If the target class is a case class, inspects the class definition
  // to extract the fields named in the constructor
  def fromObject[T <: Product with HasCodeInfo: ClassTag](obj: T): Signature = {
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

