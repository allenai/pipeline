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
  lazy val signature = Signature(name, codeVersion, dependencies, parameters)
  def checkClassEligible() = {
    require(this.getClass.getDeclaredConstructors.size == 1,"AutoSignature only works in classes with a single constructor")
    val construct = this.getClass.getDeclaredConstructors()(0)
    val x = (this.getClass.getDeclaredFields.map(_.getType),construct.getParameterTypes)
    val constructorMatchesFields = this.getClass.getDeclaredFields.map(_.getType).zip(construct.getParameterTypes).forall(t => t._1 == t._2)
    require(constructorMatchesFields, "AutoSignature only works in classes where the declared fields match the constructor arguments")
  }
  def codeVersion = this.getClass.getPackage.getImplementationVersion
  def name = this.getClass.getSimpleName
  def dependencies = {
    checkClassEligible
    val deps = for (field <- this.getClass.getDeclaredFields) yield {
      field.get(this) match {
        case s: HasSignature => Some((field.getName, s.signature))
      }
    }
    deps.flatten.toMap
  }
  def parameters = {
    checkClassEligible
    val params = for (field <- this.getClass.getDeclaredFields if !field.get(this).isInstanceOf[HasSignature]) yield {
      (field.getName, String.valueOf(field.get(this)))
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
