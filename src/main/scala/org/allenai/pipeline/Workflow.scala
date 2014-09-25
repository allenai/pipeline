package org.allenai.pipeline

import spray.json._
import spray.json.DefaultJsonProtocol._

import java.net.URI

/**
 * DAG representation of the execution of a set of Producers
 */
case class Workflow(nodes: Map[String, Node], links: Iterable[Link])

/** Represents a Producer instance with PipelineRunnerSupport */
case class Node(name: String,
                params: Map[String, String],
                outputPath: Option[URI],
                codePath: Option[URI])

/** Represents dependency between Producer instances */
case class Link(fromId: String, toId: String, name: String)

object Workflow {
  def forPipeline(steps: PipelineRunnerSupport*): Workflow = {
    def findNodes(s: PipelineRunnerSupport): Iterable[PipelineRunnerSupport] =
      Seq(s) ++ s.signature.dependencies.flatMap(t => findNodes(t._2))

    val nodeList = for {
      step <- steps
      stepInfo <- findNodes(step)
      sig = stepInfo.signature
    } yield {
      (sig.id, Node(sig.name, sig.parameters, stepInfo.outputLocation, stepInfo.codeInfo.srcUrl))
    }

    def findLinks(s: PipelineRunnerSupport): Iterable[(PipelineRunnerSupport, PipelineRunnerSupport, String)] =
      s.signature.dependencies.map { case (name, dep) => (dep, s, name)} ++
        s.signature.dependencies.flatMap(t => findLinks(t._2))

    val nodes = nodeList.toMap

    val links = (for {
      step <- steps
      (from, to, name) <- findLinks(step)
    } yield Link(from.signature.id, to.signature.id, name)).toSet
    Workflow(nodes, links)
  }

  implicit val jsFormat = {
    implicit val uriFormat = new JsonFormat[URI] {
      def write(uri: URI) = JsString(uri.toString)

      def read(value: JsValue): URI = value match {
        case JsString(uri) => new URI(uri)
        case _ => sys.error("Invalid format for URI")
      }
    }
    implicit val nodeFormat = jsonFormat4(Node)
    implicit val linkFormat = jsonFormat3(Link)
    jsonFormat2(Workflow.apply)
  }

  def link(uri: URI) = uri.getScheme match {
    case "s3" => new java.net.URI("http", s"${uri.getHost}.s3.amazonaws.com", uri.getPath,
      null).toString
    case _ => uri.toString
  }

  def limitLength(s: String, maxLength: Int = 40) =
    if (s.size < maxLength)
      s
    else {
      val leftSize = math.min(15, maxLength / 3)
      val rightSize = maxLength - leftSize
      s"${s.take(leftSize)}...${s.drop(s.size - rightSize)}"
    }

  def renderHtml(w: Workflow) = {
    val addNodeCode = (for ((id, Node(name, params, outputPath, codePath)) <- w.nodes) yield {
      val text = (name +: params.toList.map(t => s"${t._1}=${limitLength(t._2)}")).mkString(
        """\n""")
      val href = outputPath.map(p => s""", href:"${link(p)}"""").getOrElse("")
      s"""g.addNode("$id", {label: "$text" $href});"""
    }).mkString("\n")
    val addEdgeCode = (for (Link(from, to, name) <- w.links) yield {
      s"""g.addEdge(null, "$from", "$to", {label: "$name"}); """
    }).mkString("\n")
    s"""
       |<!doctype html>
       |
       |<meta charset="utf-8">
       |<title>Experiment workflow</title>
       |<h1>Experiment workflow</h1>
       |<style id="css">
       |svg {
       |    overflow: hidden;
       |}
       |
       |.node rect {
       |    stroke: #333;
       |    stroke-width: 1.5px;
       |    fill: #fff;
       |}
       |
       |.edgeLabel rect {
       |    fill: #fff;
       |}
       |
       |.edgePath {
       |    stroke: #333;
       |    stroke-width: 1.5px;
       |    fill: none;
       |}
       |</style>
       |<svg width=1650 height=1680>
       |</svg>
       |
       |<script src="http://d3js.org/d3.v3.min.js"></script>
       |<script src="http://cpettitt.github.io/project/dagre-d3/latest/dagre-d3.min.js"></script>
       |<script id="js">
       |  // Create a new directed graph
       |var g = new dagreD3.Digraph();
       |
       |// Add nodes to the graph. The first argument is the node id. The second is
       |// metadata about the node. In this case we're going to add labels to each of
       |// our nodes.
       |$addNodeCode
       |
       |// Add edges to the graph. The first argument is the edge id. Here we use null
       |// to indicate that an arbitrary edge id can be assigned automatically. The
       |// second argument is the source of the edge. The third argument is the target
       |// of the edge. The last argument is the edge metadata.
       |$addEdgeCode
       |
       |var renderer = new dagreD3.Renderer();
       |var svg = d3.select('svg'), svgGroup = svg.append('g');
       |var layout = renderer.run(g, svgGroup);
       |
       |// Center the graph
       |var xCenterOffset = (svg.attr('width') - layout.graph().width) / 2;
       |svgGroup.attr('transform', 'translate(' + xCenterOffset + ', 20)');
       |svg.attr('height', layout.graph().height + 40);
       |
       |
       |</script>
       |</head>
       |<body>
       |</body>
       |</html>
     """.stripMargin
  }
}