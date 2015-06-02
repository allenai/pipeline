package org.allenai.pipeline

import spray.json.DefaultJsonProtocol._
import spray.json._

/** Acts as an identifier for a Producer instance.  Represents the version of the implementation
  * class, the inputs, and the static configuration.  The PipelineRunner class uses a Producer's
  * Signature to  determine the path to the output data, so two Producers with the same signature
  * must always produce identical output.
  * @param name Human-readable name for the calculation done by a Producer.  Usually the class
  *             name, typically a verb
  * @param unchangedSinceVersion The latest version number at which the logic for this class
  *                              changed. Default is "0", meaning all release builds of this
  *                              class have equivalent logic
  * @param dependencies The inputs to the Producer
  * @param parameters Static configuration for the Producer.  Default is to use .toString for
  *                   constructor parameters that are not Producer instances.  If some
  *                   parameters are non-primitive types, those types should have .toString
  *                   methods that are consistent with .equals.
  */
case class Signature(
    name: String,
    unchangedSinceVersion: String,
    dependencies: Map[String, PipelineStep],
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
      val deps = s.dependencies.toList.map(t => (t._1, jsonWriter.write(t._2.stepInfo.signature))).
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

}

