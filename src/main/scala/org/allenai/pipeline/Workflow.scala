package org.allenai.pipeline

import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.Source

import java.net.URI

/** DAG representation of the execution of a set of Producers
  */
case class Workflow(nodes: Map[String, Node], links: Iterable[Link])

/** Represents a Producer instance with PipelineRunnerSupport */
case class Node(
  info: CodeInfo,
  params: Map[String, String],
  outputPath: Option[URI]
)

/** Represents dependency between Producer instances */
case class Link(fromId: String, toId: String, name: String)

object Workflow {
  private type PRS = PipelineRunnerSupport // Reduce line length
  def forPipeline(steps: PRS*): Workflow = {
    def findNodes(s: PRS): Iterable[PRS] =
      Seq(s) ++ s.signature.dependencies.flatMap(t => findNodes(t._2))

    val nodeList = for {
      step <- steps
      stepInfo <- findNodes(step)
      sig = stepInfo.signature
      codeInfo = stepInfo.codeInfo
    } yield {
      (sig.id, Node(codeInfo, sig.parameters, stepInfo.outputLocation))
    }

    def findLinks(s: PRS): Iterable[(PRS, PRS, String)] =
      s.signature.dependencies.map { case (name, dep) => (dep, s, name) } ++
        s.signature.dependencies.flatMap(t => findLinks(t._2))

    val nodes = nodeList.toMap

    val links = (for {
      step <- steps
      (from, to, name) <- findLinks(step)
    } yield Link(from.signature.id, to.signature.id, name)).toSet
    Workflow(nodes, links)
  }

  implicit val jsFormat = {
    import CodeInfo._
    implicit val nodeFormat = jsonFormat3(Node)
    implicit val linkFormat = jsonFormat3(Link)
    jsonFormat2(Workflow.apply)
  }

  private def link(uri: URI) = uri.getScheme match {
    case "s3" => new java.net.URI("http", s"${uri.getHost}.s3.amazonaws.com", uri.getPath,
      null).toString
    case _ => uri.toString
  }

  private val DEFAULT_MAX_SIZE = 40
  private val LHS_MAX_SIZE = 15
  private def limitLength(s: String, maxLength: Int = DEFAULT_MAX_SIZE) =
    if (s.size < maxLength) {
      s
    } else {
      val leftSize = math.min(LHS_MAX_SIZE, maxLength / 3)
      val rightSize = maxLength - leftSize
      s"${s.take(leftSize)}...${s.drop(s.size - rightSize)}"
    }

  def renderHtml(w: Workflow): String = {
    val addNodeCode = (for ((id, Node(info, params, outputPath)) <- w.nodes) yield {
      val paramsText = params.toList.map {
        case (key, value) =>
          s""""$key=${limitLength(value)}""""
      }.mkString(",")
      val linksText = (info.srcUrl.map(uri => s"""new Link("${link(uri)}","v${info.buildId}")""") ++
        outputPath.map(uri => s"""new Link("${link(uri)}","output")""")).mkString(",")
      s"""g.addNode("$id", {\n
         |label: generateStepContent("${info.className}",\n
         |[$paramsText],\n
         |[$linksText]\n
         |)});""".stripMargin
    }).mkString("\n")
    val addEdgeCode = (for (Link(from, to, name) <- w.links) yield {
      s"""g.addEdge(null, "$from", "$to", {label: "$name"}); """
    }).mkString("\n")
    val template = Source.fromURL(this.getClass.getResource("/pipelineSummary.html")).mkString
    template.format(addNodeCode, addEdgeCode)
  }
}
