package org.allenai.pipeline

import spray.json._
import DefaultJsonProtocol._

import spray.json._

/**
 * Created by rodneykinney on 9/3/14.
 */
case class Signature(name: String,
                     codeVersion: String,
                     dependencies: Map[String, Signature],
                     parameters: Map[String, String]) {
  def id = this.toJson(Signature.JsonFormat).compactPrint.hashCode

  def infoString = this.toJson(Signature.JsonFormat).prettyPrint

}

trait HasSignature {
  def signature: Signature
}

object Signature {

  object JsonFormat extends JsonFormat[Signature] {
    def write(s: Signature) = {
      val deps = JsObject(
        s.dependencies.toList.map(t => (t._1, JsonFormat.write(t._2))).sortBy(_._1))
      val params = s.parameters.toList.sortBy(_._1).toJson
      JsArray(JsString(s.codeVersion), JsString(s.name), deps, params)
    }

    def read(value: JsValue): Signature = value match {
      case JsArray(List(JsString(_codeVersion), JsString(_name), deps, params)) => {
        val _dependencies = deps.convertTo[List[(String, JsValue)]].map(t => (t._1,
          JsonFormat.read(t._2))).toMap
        val _parameters = params.convertTo[List[(String, String)]].toMap
        Signature(codeVersion = _codeVersion, name = _name,
          dependencies = _dependencies, parameters = _parameters)
      }
      case _ => deserializationError("Invalid format for Signature")
    }
  }

  def from(base: Any, params: (String, Any)*) = {
    val (deps, pars) = params.partition(_._2.isInstanceOf[HasSignature])
    Signature(codeVersion = codeVersionOf(base),
      name = base.getClass.getSimpleName,
      dependencies = deps.map { case (name, p: HasSignature) => (name, p.signature)}.toMap,
      parameters = pars.map { case (name, value) => (name, String.valueOf(value))}.toMap
    )
  }

  def auto(obj: Product) = {
    def declaredFields() = {
      val f = obj.getClass.getDeclaredFields
      f.foreach(_.setAccessible(true))
      f.map(_.getName).zip(f.map(_.get(obj)))
    }

    def dependencies() = {
      val deps = for ((name, d: HasSignature) <- declaredFields) yield (name, d.signature)
      deps.toMap
    }

    def parameters() = {
      val params = for ((name, value) <- declaredFields if !value.isInstanceOf[HasSignature]) yield {
        (name, String.valueOf(value))
      }
      params.toMap
    }
    Signature(codeVersion = codeVersionOf(obj),
      name = obj.getClass.getSimpleName,
      dependencies = dependencies(),
      parameters = parameters())

  }

}
