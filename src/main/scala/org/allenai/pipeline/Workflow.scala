package org.allenai.pipeline

import org.allenai.common.Resource

import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.io.Source

import java.net.URI

/** DAG representation of the execution of a set of Producers.
  */
case class Workflow(nodes: Map[String, Node], links: Iterable[Link]) {
  def sourceNodes() = nodes.filter {
    case (nodeId, node) =>
      !links.exists(link => link.toId == nodeId)
  }

  def sinkNodes() = nodes.filter {
    case (nodeId, node) =>
      !links.exists(link => link.fromId == nodeId)
  }
}

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
    val sourceNodes = w.sourceNodes()
    val sinkNodes = w.sinkNodes()
    val addNodes =
      for ((id, Node(info, params, outputPath)) <- w.nodes) yield {
        // Params show up as line items in the pipeline diagram node.
        val paramsText = params.toList.map {
          case (key, value) =>
            s""""$key=${limitLength(value)}""""
        }.mkString(",")
        // A link is like a param but it hyperlinks somewhere.
        val links =
          // An optional link to the source data.
          info.srcUrl.map(uri => s"""new Link("${link(uri)}","v${info.buildId}")""") ++
            // An optional link to the output data.
            outputPath.map(uri => s"""new Link("${link(uri)}","output")""")
        val clazz = sourceNodes match {
          case _ if sourceNodes contains id => "sourceNode"
          case _ if sinkNodes contains id => "sinkNode"
          case _ => ""
        }
        val linksText = links.mkString(",")
        s"""        g.setNode("$id", {
           |          class: "$clazz",
           |          labelType: "html",
           |          label: generateStepContent("${info.className}",
           |            [$paramsText],
           |            [$linksText])
           |        });""".stripMargin
      }
    val addEdges =
      for (Link(from, to, name) <- w.links) yield {
        s"""        g.setEdge("$from", "$to", {label: "$name"}); """
      }

    val resourceName = "pipelineSummary.html"
    val resourceUrl = this.getClass.getResource(resourceName)
    require(resourceUrl != null, s"Could not find resource: ${resourceName}")
    val template = Resource.using(Source.fromURL(resourceUrl)) { source =>
      source.mkString
    }
    template.format(addNodes.mkString("\n\n"), addEdges.mkString("\n\n"))
  }
}
