package org.allenai.pipeline

import spray.json._
import spray.json.DefaultJsonProtocol._

import java.net.URI

/** DAG representation of the execution of a set of Producers
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
    implicit val uriFormat = new JsonFormat[URI] {
      override def write(uri: URI): JsValue = JsString(uri.toString)

      override def read(value: JsValue): URI = value match {
        case JsString(uri) => new URI(uri)
        case _ => sys.error("Invalid format for URI")
      }
    }
    implicit val nodeFormat = jsonFormat4(Node)
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
    val addNodeCode = (for ((id, Node(name, params, outputPath, codePath)) <- w.nodes) yield {
      val paramsText = params.toList.map {
        case (key, value) =>
          s""""$key=${limitLength(value)}""""
      }.mkString(",")
      val linksText = (codePath.map(uri => s"""new Link("${link(uri)}","code")""") ++
        outputPath.map(uri => s"""new Link("${link(uri)}","output")""")).mkString(",")
      s"""g.addNode("$id", {\n
         |label: generateStepContent("$name",\n
         |[$paramsText],\n
         |[$linksText]\n
         |)});""".stripMargin
    }).mkString("\n")
    val addEdgeCode = (for (Link(from, to, name) <- w.links) yield {
      s"""g.addEdge(null, "$from", "$to", {label: "$name"}); """
    }).mkString("\n")
    // scalastyle:off
    s"""<!DOCTYPE html><html>
      |  <head>
      |    <meta charset="utf-8">
      |    <title>Experiment Workflow</title>
      |    <style type="text/css">
      |      html{font-size:100%;padding:40px;background:#2a75a1}svg{background:#fff;padding:40px;box-sizing:border-box;overflow:scroll;box-shadow:0 0 5px #000}.node div{padding:20px}.node div h2{margin:0;font-size:1.25em;text-align:center}.node ul{list-style-type:none;margin:10px 0;padding:0}.node ul li{margin:10px 0}.node ul li:first-child{margin-top:0}.node ul li:last-child{margin-bottom:0}.node .links{border-top:1px solid #2a75a1;padding-top:10px}.node rect{stroke:#2a75a1;stroke-width:2px;fill:#e2f0f8}.node a{color:#2a75a1}.node a:hover{color:#489dcf}.edgeLabel rect{fill:#fff}.edgePath{stroke:#2a75a1;stroke-width:2px;fill:none}/*! normalize.css v3.0.1 | MIT License | git.io/normalize */html{font-family:sans-serif;-ms-text-size-adjust:100%;-webkit-text-size-adjust:100%}body{margin:0}article,aside,details,figcaption,figure,footer,header,hgroup,main,nav,section,summary{display:block}audio,canvas,progress,video{display:inline-block;vertical-align:baseline}audio:not([controls]){display:none;height:0}[hidden],template{display:none}a{background:0 0}a:active,a:hover{outline:0}abbr[title]{border-bottom:1px dotted}b,strong{font-weight:700}dfn{font-style:italic}h1{font-size:2em;margin:.67em 0}mark{background:#ff0;color:#000}small{font-size:80%}sub,sup{font-size:75%;line-height:0;position:relative;vertical-align:baseline}sup{top:-.5em}sub{bottom:-.25em}img{border:0}svg:not(:root){overflow:hidden}figure{margin:1em 40px}hr{-moz-box-sizing:content-box;box-sizing:content-box;height:0}pre{overflow:auto}code,kbd,pre,samp{font-family:monospace,monospace;font-size:1em}button,input,optgroup,select,textarea{color:inherit;font:inherit;margin:0}button{overflow:visible}button,select{text-transform:none}button,html input[type=button],input[type=reset],input[type=submit]{-webkit-appearance:button;cursor:pointer}button[disabled],html input[disabled]{cursor:default}button::-moz-focus-inner,input::-moz-focus-inner{border:0;padding:0}input{line-height:normal}input[type=checkbox],input[type=radio]{box-sizing:border-box;padding:0}input[type=number]::-webkit-inner-spin-button,input[type=number]::-webkit-outer-spin-button{height:auto}input[type=search]{-webkit-appearance:textfield;-moz-box-sizing:content-box;-webkit-box-sizing:content-box;box-sizing:content-box}input[type=search]::-webkit-search-cancel-button,input[type=search]::-webkit-search-decoration{-webkit-appearance:none}fieldset{border:1px solid silver;margin:0 2px;padding:.35em .625em .75em}legend{border:0;padding:0}textarea{overflow:auto}optgroup{font-weight:700}table{border-collapse:collapse;border-spacing:0}td,th{padding:0}
      |    </style>
      |  </head>
      |  <body>
      |    <svg width="100%"></svg>
      |    <script src="http://d3js.org/d3.v3.min.js"></script>
      |    <script src="http://cpettitt.github.io/project/dagre-d3/latest/dagre-d3.min.js"></script>
      |    <script>
      |      (function() {
      |        /**
      |         * Class representing an individual Link.
      |         */
      |        function Link(href, text) {
      |          this.href = href;
      |          this.text = text;
      |        };
      |
      |        Link.prototype.toHtml = function() {
      |          return '<a href="' + this.href + '" target="_blank">' + this.text + '</a>';
      |        };
      |
      |        /**
      |         * Generates the content for a step of a step.
      |         *
      |         * @param {string}    label The step label.
      |         * @param {string[]}  data  Array of data points to display in the step.
      |         * @param {Link[]}    links Array of links to display in the step.
      |         *
      |         * @return {string}   The HTML for the step contents.
      |         */
      |        function generateStepContent(label, data, links) {
      |          var out = "<h2>" + label + "</h2>";
      |          if(data && Array.isArray(data)) {
      |            out += '<ul class="data">';
      |            data.forEach(function(d) {
      |              out += '<li>' + d + '</li>';
      |            });
      |            out += '</ul>';
      |            if(links) {
      |              out += '<ul class="links">';
      |              links.forEach(function(l) {
      |                if(l instanceof Link) {
      |                  out += '<li>' + l.toHtml() + '</li>';
      |                }
      |              });
      |              out += '</ul>';
      |            }
      |          }
      |          return out;
      |        };
      |
      |
      |        // Create a new directed graph
      |        var g = new dagreD3.Digraph();
      |
      |        // Add nodes to the graph. The first argument is the node id. The second is
      |        // metadata about the node. In this case we're going to add labels to each of
      |        // our nodes.
      |        $addNodeCode
      |
      |        // Add edges to the graph. The first argument is the edge id. Here we use null
      |        // to indicate that an arbitrary edge id can be assigned automatically. The
      |        // second argument is the source of the edge. The third argument is the target
      |        // of the edge. The last argument is the edge metadata.
      |        $addEdgeCode
      |
      |        var renderer = new dagreD3.Renderer();
      |        var svg = d3.select('svg');
      |        var svgGroup = svg.append('g');
      |        var layout = renderer.run(g , svgGroup);
      |
      |        // Center the graph
      |        var xCenterOffset = (svg[0][0].clientWidth - layout.graph().width) / 2;
      |        svgGroup.attr('transform', 'translate(' + xCenterOffset + ', 20)');
      |        svg.attr('height', layout.graph().height + 120);
      |      })();
      |    </script>
      |  </body>
      |</html>
      |""".stripMargin
    // scalastyle:on
  }
}
