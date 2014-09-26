package org.allenai.pipeline

import spray.json._
import DefaultJsonProtocol._

import spray.json._

import scala.reflect.ClassTag

case class Signature(name: String,
                     unchangedSinceVersion: String,
                     dependencies: Map[String, PipelineRunnerSupport],
                     parameters: Map[String, String]) {
  def id: String = {
    val hashString = this.toJson.compactPrint
    val hashCodeLong = hashString.foldLeft(0L){(hash,  char) => hash * 31 + char}
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

    def write(s: Signature) = {
      // Sort keys in dependencies and parameters so that json format is identical for equal objects
      val deps = s.dependencies.toList.map(t => (t._1, jsonWriter.write(t._2.signature))).
        sortBy(_._1).toJson
      val params = s.parameters.toList.sortBy(_._1).toJson
      JsObject((NAME, JsString(s.name)),
        (CODE_VERSION_ID, JsString(s.unchangedSinceVersion)),
        (DEPENDENCIES, deps),
        (PARAMETERS, params))
    }
  }

  def apply(name: String, unchangedSinceVersion: String, params: (String, Any)*): Signature = {
    val (deps, pars) = params.partition(_._2.isInstanceOf[PipelineRunnerSupport])
    Signature(name = name,
      unchangedSinceVersion = unchangedSinceVersion,
      dependencies = deps.map { case (n, p: PipelineRunnerSupport) => (n, p)}.toMap,
      parameters = pars.map { case (n, value) => (n, String.valueOf(value))}.toMap
    )
  }

  import scala.reflect.runtime.universe._

  def fromFields(base: HasCodeInfo,
                 fieldNames: String*): Signature =
    fromInfoAndFields(base.codeInfo, base, fieldNames: _*)

  def fromInfoAndFields(info: CodeInfo,
                        base: Any,
                        fieldNames: String*): Signature = {
    val params = for (field <- fieldNames) yield {
      val f = base.getClass.getDeclaredField(field)
      f.setAccessible(true)
      (field, f.get(base))
    }
    apply(base.getClass.getSimpleName, info.unchangedSince, params: _*)
  }

  def fromObject[T <: Product with HasCodeInfo : TypeTag : ClassTag](obj: T): Signature = {
    val objType = typeTag[T].tpe
    val constructor = objType.member(nme.CONSTRUCTOR).asMethod
    val constructorParams = constructor.paramss.head
    val declarations = constructorParams.map(p => objType.declaration(newTermName(p.name
      .toString)).asTerm)
    val reflect = typeTag[T].mirror.reflect(obj)
    val paramValues = declarations.map(d => (d.name.toString, reflect.reflectField(d).get))
    val (deps, params) = paramValues.partition(_._2.isInstanceOf[PipelineRunnerSupport])
    Signature(objType.typeSymbol.name.toString,
      obj.codeInfo.unchangedSince,
      deps.map(t => (t._1, t._2.asInstanceOf[PipelineRunnerSupport])).toMap,
      params.map(t => (t._1, t._2.toString)).toMap)
  }
}

