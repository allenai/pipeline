package org.allenai.pipeline

import spray.json._
import DefaultJsonProtocol._

import spray.json._

/**
 * Created by rodneykinney on 9/3/14.
 */
case class Signature(name: String,
                     codeVersion: String,
                     dependencies: Map[String, HasSignature],
                     parameters: Map[String, String]) extends HasSignature {
  def id: String = this.toJson(Signature.JsonFormat).compactPrint.hashCode.toHexString

  def infoString: String = this.toJson(Signature.JsonFormat).prettyPrint

  override def signature: Signature = this

}

trait HasSignature {
  def signature: Signature
}

object Signature {

  implicit object JsonFormat extends JsonFormat[Signature] {
    private val CODE_VERSION = "codeVersion"
    private val NAME = "name"
    private val DEPENDENCIES = "dependencies"
    private val PARAMETERS = "parameters"

    def write(s: Signature) = {
      // Sort keys in dependencies and parameters so that json format is identical for equal objects
      val deps = s.dependencies.toList.map(t => (t._1, JsonFormat.write(t._2.signature))).
        sortBy(_._1).toJson
      val params = s.parameters.toList.sortBy(_._1).toJson
      JsObject((CODE_VERSION, JsString(s.codeVersion)),
        (NAME, JsString(s.name)),
        (DEPENDENCIES, deps),
        (PARAMETERS, params))
    }

    def read(value: JsValue): Signature = value match {
      case JsObject(fields) => {
        (fields.get(CODE_VERSION),
          fields.get(NAME),
          fields.get(DEPENDENCIES),
          fields.get(PARAMETERS)) match {
          case (Some(JsString(_codeVersion)),
          Some(JsString(_name)),
          Some(deps),
          Some(params)) => {
            val _dependencies = deps.convertTo[List[(String, JsValue)]].
              map { case (n, v) => (n, JsonFormat.read(v))}.toMap
            val _parameters = params.convertTo[List[(String, String)]].toMap
            Signature(codeVersion = _codeVersion, name = _name,
              dependencies = _dependencies, parameters = _parameters)
          }
          case _ => deserializationError(s"Invalid format for Signature: $value")
        }
      }
      case _ => deserializationError(s"Invalid format for Signature: $value")
    }
  }

  def fromParameters(base: Any, params: (String, Any)*) = {
    val (deps, pars) = params.partition(_._2.isInstanceOf[HasSignature])
    Signature(codeVersion = codeVersionOf(base),
      name = base.getClass.getSimpleName,
      dependencies = deps.map { case (name, p: HasSignature) => (name, p.signature)}.toMap,
      parameters = pars.map { case (name, value) => (name, String.valueOf(value))}.toMap
    )
  }

  def fromObject(obj: Product) = {
    def declaredFields() = {
      val f = obj.getClass.getDeclaredFields
      f.foreach(_.setAccessible(true))
      f.map(_.getName).zip(f.map(_.get(obj)))
    }

    def dependencies() = {
      val deps = for ((name, d: HasSignature) <- declaredFields) yield (name, d)
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
