package org.allenai.pipeline

import spray.json._

/**
 * Created by rodneykinney on 9/3/14.
 */
case class Signature(codeVersion: String, name: String, dependencies: Iterable[Signature], parameters: Iterable[(String, String)])

object Signature {

  object JsonFormat extends JsonFormat[Signature] {
    import DefaultJsonProtocol._
    def write(s: Signature) = {
      val deps = JsArray(s.dependencies.map(JsonFormat.write).toList)
      val params = s.parameters.toJson
      JsArray(JsString(s.codeVersion), JsString(s.name), deps, params)
    }

    def read(value: JsValue): Signature = value match {
      case JsArray(List(JsString(codeVersion), JsString(name), JsArray(deps), params)) => {
        val dependencies = deps.map(JsonFormat.read)
        val parameters = params.convertTo[List[(String, String)]]
        Signature(codeVersion, name, dependencies, parameters)
      }
      case _ => deserializationError("Invalid format for Signature")
    }
  }

}

trait HasSignature {
  def signature: Signature
}
