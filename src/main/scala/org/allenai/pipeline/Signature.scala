package org.allenai.pipeline

import spray.json._
import DefaultJsonProtocol._

import spray.json._

import scala.reflect.ClassTag

/**
 * Created by rodneykinney on 9/3/14.
 */
case class Signature(name: String,
                     dependencies: Map[String, HasSignature],
                     parameters: Map[String, String]) extends HasSignature {
  def id: String = this.toJson.compactPrint.hashCode.toHexString

  def infoString: String = this.toJson.prettyPrint

  override def signature: Signature = this

}

trait HasSignature {
  def signature: Signature
}

object Signature {

  implicit val jsonFormat: JsonFormat[Signature] = new JsonFormat[Signature] {
    private val NAME = "name"
    private val DEPENDENCIES = "dependencies"
    private val PARAMETERS = "parameters"

    def write(s: Signature) = {
      // Sort keys in dependencies and parameters so that json format is identical for equal objects
      val deps = s.dependencies.toList.map(t => (t._1, jsonFormat.write(t._2.signature))).
        sortBy(_._1).toJson
      val params = s.parameters.toList.sortBy(_._1).toJson
      JsObject((NAME, JsString(s.name)),
        (DEPENDENCIES, deps),
        (PARAMETERS, params))
    }

    def read(value: JsValue): Signature = value match {
      case JsObject(fields) => {
        (fields.get(NAME),
          fields.get(DEPENDENCIES),
          fields.get(PARAMETERS)) match {
          case (Some(JsString(_name)),
          Some(deps),
          Some(params)) => {
            val _dependencies = deps.convertTo[List[(String, JsValue)]].
              map { case (n, v) => (n, jsonFormat.read(v))}.toMap
            val _parameters = params.convertTo[List[(String, String)]].toMap
            Signature(name = _name,
              dependencies = _dependencies,
              parameters = _parameters)
          }
          case _ => deserializationError(s"Invalid format for Signature: $value")
        }
      }
      case _ => deserializationError(s"Invalid format for Signature: $value")
    }
  }

  def apply(name: String, params: (String, Any)*): Signature = {
    val (deps, pars) = params.partition(_._2.isInstanceOf[HasSignature])
    Signature(name = name,
      dependencies = deps.map { case (name, p: HasSignature) => (name, p.signature)}.toMap,
      parameters = pars.map { case (name, value) => (name, String.valueOf(value))}.toMap
    )
  }

  import scala.reflect.runtime.universe._

  def fromFields(base: Any, fieldNames: String*): Signature = {
    val params = for (field <- fieldNames) yield {
      val f = base.getClass.getDeclaredField(field)
      f.setAccessible(true)
      (field, f.get(base))
    }
    apply(base.getClass.getSimpleName, params: _*)
  }

  def fromObject[T <: Product : TypeTag : ClassTag](obj: T): Signature = {
    val objType = typeTag[T].tpe
    val constructor = objType.member(nme.CONSTRUCTOR).asMethod
    val constructorParams = constructor.paramss.head
    val declarations = constructorParams.map(p => objType.declaration(newTermName(p.name
      .toString)).asTerm)
    val reflect = typeTag[T].mirror.reflect(obj)
    val paramValues = declarations.map(d => (d.name.toString, reflect.reflectField(d).get))
    val (deps, params) = paramValues.partition(_._2.isInstanceOf[HasSignature])
    Signature(objType.typeSymbol.name.toString,
      deps.map(t => (t._1, t._2.asInstanceOf[HasSignature])).toMap,
      params.map(t => (t._1, t._2.toString)).toMap)
  }
}

