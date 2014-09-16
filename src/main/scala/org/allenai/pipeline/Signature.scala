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
  def id: String = this.toJson(Signature.JsonFormat).compactPrint.hashCode.toHexString

  def infoString: String = this.toJson(Signature.JsonFormat).prettyPrint

  override def signature: Signature = this

}

trait HasSignature {
  def signature: Signature
}

object Signature {

  implicit object JsonFormat extends JsonFormat[Signature] {
    private val NAME = "name"
    private val DEPENDENCIES = "dependencies"
    private val PARAMETERS = "parameters"

    def write(s: Signature) = {
      // Sort keys in dependencies and parameters so that json format is identical for equal objects
      val deps = s.dependencies.toList.map(t => (t._1, JsonFormat.write(t._2.signature))).
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
              map { case (n, v) => (n, JsonFormat.read(v))}.toMap
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

  def fromParameters(base: Any, params: (String, Any)*) = {
    val (deps, pars) = params.partition(_._2.isInstanceOf[HasSignature])
    Signature(name = base.getClass.getSimpleName,
      dependencies = deps.map { case (name, p: HasSignature) => (name, p.signature)}.toMap,
      parameters = pars.map { case (name, value) => (name, String.valueOf(value))}.toMap
    )
  }

  import scala.reflect.runtime.universe._

  def fromConstructor[T: TypeTag : ClassTag](obj: T): Signature = {
    val objType = typeTag[T].tpe
    val constructor = objType.member(nme.CONSTRUCTOR).asMethod
    val constructorParams = constructor.paramss.head
    val declarations = constructorParams.map(p => objType.declaration(newTermName(p.name
      .toString)).asTerm)
    val reflect = runtimeMirror(obj.getClass.getClassLoader).reflect(obj)
    val paramValues = declarations.map(d => (d.name.toString, reflect.reflectField(d).get))
    Signature(objType.toString, Map(), paramValues.map(t => (t._1,t._2.toString)).toMap)
  }

  def fromObject(obj: Product): Signature = {
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
    Signature(name = obj.getClass.getSimpleName,
      dependencies = dependencies(),
      parameters = parameters())

  }

}

