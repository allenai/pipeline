package org.allenai.pipeline

import spray.json._
import DefaultJsonProtocol._

import spray.json._

/**
 * Created by rodneykinney on 9/3/14.
 */
case class Signature(name: String, codeVersion: String, dependencies: Map[String, Signature], parameters: Map[String, String]) {
  def id = this.toJson(Signature.JsonFormat).compactPrint.hashCode
  def infoString = this.toJson(Signature.JsonFormat).prettyPrint
}

trait AutoSignature extends HasSignature {
  outer =>
  def fields: Set[String]
  lazy val signature = Signature(name, codeVersion, dependencies, parameters)
  def codeVersion = {
    val v = outer.getClass.getPackage.getImplementationVersion
    if (v == null) "compiled-locally" else v
  }
  def name = outer.getClass.getSimpleName
  private def declaredFields = {
    val f = outer.getClass.getDeclaredFields.filter(f => fields(f.getName))
    f.foreach(_.setAccessible(true))
    f.map(_.getName).zip(f.map(_.get(outer)))
  }
  def dependencies = {
    val deps = for ((name, d: HasSignature) <- declaredFields) yield (name, d.signature)
    deps.toMap
  }
  def parameters = {
    val params = for ((name, value)  <- declaredFields if !value.isInstanceOf[HasSignature]) yield {
      (name, String.valueOf(value))
    }
    params.toMap
  }
}

object Signature {

  object JsonFormat extends JsonFormat[Signature] {
    def write(s: Signature) = {
      val deps = JsObject(s.dependencies.toList.map(t => (t._1, JsonFormat.write(t._2))))
      val params = s.parameters.toJson
      JsArray(JsString(s.codeVersion), JsString(s.name), deps, params)
    }

    def read(value: JsValue): Signature = value match {
      case JsArray(List(JsString(codeVersion), JsString(name), deps, params)) => {
        val dependencies = deps.convertTo[List[(String, JsValue)]].map(t => (t._1, JsonFormat.read(t._2))).toMap
        val parameters = params.convertTo[Map[String, String]]
        Signature(codeVersion, name, dependencies, parameters)
      }
      case _ => deserializationError("Invalid format for Signature")
    }
  }

}

trait HasSignature {
  def signature: Signature
}
