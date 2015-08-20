package org.allenai.pipeline

import java.net.URI

import org.allenai.common.Resource
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.io.Source

/** DAG representation of the execution of a set of Producers.
  */
case class Workflow(nodes: Map[String, Node], links: Iterable[Link], title: String, pipescripts: Option[PipescriptSources] = None) {
  def sourceNodes() = nodes.filter {
    case (nodeId, node) =>
      !links.exists(link => link.toId == nodeId)
  }

  def sinkNodes() = nodes.filter {
    case (nodeId, node) =>
      !links.exists(link => link.fromId == nodeId)
  }

  def errorNodes() = nodes.filter {
    case (nodeId, node) => node.outputMissing
  }

  lazy val renderHtml: String = {
    import Workflow._
    //    val w = this
    val sources = sourceNodes()
    val sinks = sinkNodes()
    val errors = errorNodes()
    // Collect nodes with output paths to be displayed in the upper-left.
    val outputNodeLinks = for {
      (id, info) <- nodes.toList.sortBy(_._2.stepName)
      link <- info.outputLocation.map(displayLinks)
    } yield {
      link match {
        case Right(path) => s"""<a href="$path">${info.stepName}</a>"""
        case Left(paths) =>
          val links = paths.map {
            case (name, path) => s"""<a href="$path">$name</a>"""
          }
          s"""${info.stepName} (${links.mkString(" ")})"""
      }
    }
    val addNodes =
      for ((id, info) <- nodes) yield {
        // Params show up as line items in the pipeline diagram node.
        val params = info.parameters.toList.map {
          case (key, value) =>
            s"$key=${unescape(limitLength(value))}"
        }
        // A link is like a param but it hyperlinks somewhere.
        val links =
          // An optional link to the source data.
          info.srcUrl.map(uri => s"""new Link(${toHttp(uri).toString.toJson},${(if (info.classVersion.nonEmpty) info.classVersion else "src").toJson})""") ++ // scalastyle:ignore
            // An optional link to the output data.
            info.outputLocation.toList.flatMap { loc =>
              displayLinks(loc) match {
                case Right(url) => List(s"""new Link(${url.toString.toJson},"data")""")
                case Left(urls) =>
                  urls.map {
                    case (name, url) => s"""new Link(${url.toString.toJson},"data ($name)")"""
                  }
              }
            }
        val linksJson = links.mkString("[", ",", "]")
        val clazz = sources match {
          case _ if errors contains id => "errorNode"
          case _ if sources contains id => "sourceNode"
          case _ if sinks contains id => "sinkNode"
          case _ => ""
        }
        val name = info.stepName
        val desc = {
          // Word wrap the description
          val text = unescape(info.description.getOrElse(if (name == info.className) "" else info.className))
            .split("""\s+""").map(linkUrlsInString)
          val textLines = new ListBuffer[String]
          val currentLine = new StringBuilder
          for (idx <- 0 until text.size) {
            if (currentLine.toString.size > 0 && currentLine.toString.size + text(idx).size > DEFAULT_MAX_SIZE) {
              textLines += currentLine.toString
              currentLine.clear()
              currentLine.append(text(idx))
            } else {
              if (currentLine.toString.size > 0) currentLine.append(" ")
              currentLine.append(text(idx))
            }
          }
          if (currentLine.toString.size > 0) textLines += currentLine.toString
          textLines.mkString("<ul><li>", "</li><li>", "</li></ul>")
        }
        s"""        g.setNode("$id", {
                                    |       class: "$clazz",
                                                            |       labelType: "html",
                                                            |       label: generateStepContent(${name.toJson},
                                                                                                               |         ${desc.toJson},
                                                                                                                                         |         ${info.executionInfo.toJson},
                                                                                                                                                                                 |         ${params.toJson},
                                                                                                                                                                                                             |         ${linksJson})
                                                                                                                                                                                                                                     |     });""".stripMargin
      }
    val addEdges =
      for (Link(from, to, name) <- links) yield {
        s"""        g.setEdge("$from", "$to", {lineInterpolate: 'basis', label: "$name"}); """
      }

    val resourceName = "pipelineSummary.html"
    val resourceUrl = this.getClass.getResource(resourceName)
    require(resourceUrl != null, s"Could not find resource: ${resourceName}")
    val template = Resource.using(Source.fromURL(resourceUrl)) { source =>
      source.mkString
    }
    val portableTitle = title.replace(".pipe", ".portable.pipe")
    val outputNodeHtml = outputNodeLinks.map("<li>" + _ + "</li>").mkString("<ul>", "\n", "</ul>")
    val pipescriptsHtml = (pipescripts map {
      case PipescriptSources(original, stable) =>
        s"""<div class="typescripts">
<ul>
<li><a href="$original" type="text/plain" download="$title">Original</a></li>
<li><a href="$stable" type="text/plain" download="$portableTitle">Portable</a></li>
</ul></div>
"""
    }).getOrElse("")

    template.format(title, pipescriptsHtml, outputNodeHtml, addNodes.mkString("\n\n"), addEdges.mkString("\n\n"))
  }
}

/** Represents a PipelineStep without its dependencies */
case class Node(
  stepName: String,
  className: String,
  classVersion: String = "",
  srcUrl: Option[URI] = None,
  binaryUrl: Option[URI] = None,
  parameters: Map[String, String] = Map(),
  description: Option[String] = None,
  outputLocation: Option[URI] = None,
  outputMissing: Boolean = false,
  executionInfo: String = ""
)

object Node {
  def apply(stepName: String, step: PipelineStep): Node = {
    val stepInfo = step.stepInfo
    val outputMissing = step match {
      case persisted: PersistedProducer[_, _] =>
        !persisted.artifact.exists
      case _ => false
    }
    val executionInfo = step match {
      case producer: Producer[_] => producer.executionInfo.status
      case _ => ""
    }
    Node(
      stepName,
      stepInfo.className,
      stepInfo.classVersion,
      stepInfo.srcUrl,
      stepInfo.binaryUrl,
      stepInfo.parameters,
      stepInfo.description,
      stepInfo.outputLocation,
      outputMissing,
      executionInfo
    )
  }
}

/** Represents dependency between Producer instances */
case class Link(fromId: String, toId: String, name: String)

object Workflow {
  def forPipeline(steps: Iterable[(String, PipelineStep)], targets: Iterable[String], title: String, pipescripts: Option[PipescriptSources]): Workflow = {
    val idToName = steps.map { case (k, v) => (v.stepInfo.signature.id, k) }.toMap
    val nameToStep = steps.toMap
    def findNodes(s: PipelineStep): Iterable[PipelineStep] =
      Seq(s) ++ s.stepInfo.dependencies.flatMap {
        case (name, step) =>
          findNodes(step)
      }

    val nodeList = for {
      name <- targets
      step = nameToStep(name)
      childStep <- findNodes(step)
    } yield {
      val id = childStep.stepInfo.signature.id
      val childName = idToName.getOrElse(id, childStep.stepInfo.className)
      (id, Node(childName, childStep))
    }

    def findLinks(s: PipelineStepInfo): Iterable[(PipelineStepInfo, PipelineStepInfo, String)] =
      s.dependencies.map { case (name, dep) => (dep.stepInfo, s, name) } ++
        s.dependencies.flatMap(t => findLinks(t._2.stepInfo))

    val nodes = nodeList.toMap

    val links = (for {
      stepName <- targets
      step = nameToStep(stepName)
      (from, to, name) <- findLinks(step.stepInfo)
    } yield Link(from.signature.id, to.signature.id, name)).toSet
    Workflow(nodes, links, title, pipescripts)
  }

  def upstreamDependencies(step: PipelineStep): Set[PipelineStep] = {
    val parents = step.stepInfo.dependencies.map(_._2).toSet
    parents ++ parents.flatMap(upstreamDependencies)
  }

  implicit val jsFormat = {
    implicit val linkFormat = jsonFormat3(Link)
    implicit val uriFormat = new JsonFormat[URI] {
      override def write(uri: URI): JsValue = JsString(uri.toString)

      override def read(value: JsValue): URI = value match {
        case JsString(uri) => new URI(uri)
        case s => sys.error(s"Invalid URI: $s")
      }
    }
    implicit val nodeFormat = jsonFormat10(Node.apply)
    implicit val pipescriptsFormat = jsonFormat2(PipescriptSources.apply)
    jsonFormat(Workflow.apply, "nodes", "links", "title", "pipescripts")
  }

  private def toHttp(uri: URI) = uri.getScheme match {
    case "s3" | "s3n" =>
      new java.net.URI("http", s"${uri.getHost}.s3.amazonaws.com", uri.getPath, null)
    case "file" =>
      new java.net.URI(null, null, uri.getPath, null)
    case _ => uri
  }

  private def displayLinks(url: URI) = {
    url.getScheme match {
      case "s3" | "s3n" =>
        Left(List(
          ("s3:", url),
          ("http:", toHttp(url))
        ))
      case _ =>
        Right(url)
    }
  }

  private val urlRegex = """https?://[^\s]+""".r
  private[pipeline] def linkUrlsInString(string: String) = {
    urlRegex.replaceAllIn(string, m => s"<a href='${m.group(0)}'>${limitLength(m.group(0))}</a>")
  }

  private val DEFAULT_MAX_SIZE = 40
  private val LHS_MAX_SIZE = 15

  private[pipeline] def limitLength(s: String, maxLength: Int = DEFAULT_MAX_SIZE) = {
    val trimmed = if (s.size < maxLength) {
      s
    } else {
      val leftSize = math.min(LHS_MAX_SIZE, maxLength / 3)
      val rightSize = maxLength - leftSize
      s"${s.take(leftSize)}...${s.drop(s.size - rightSize)}"
    }
    trimmed
  }
  private def unescape(s: String) = s.replaceAll(">", "&gt;").replaceAll("<", "&lt;")
}
