package org.allenai.pipeline

/**
 * DAG representation of the execution of a set of Producers
 */
class Workflow(steps: HasSignature*) {
  private val nodeList = for {
    step <- steps
    hs <- findNodes(step)
    sig = hs.signature
  } yield hs match {
      case p: HasPath => (sig.id, Node(sig.id, sig.name, sig.parameters, Some(p.path)))
      case _ => (sig.id, Node(sig.id, sig.name, sig.parameters, None))
  }
  
  val nodes = nodeList.toMap
  
  val links = for {
    step <- steps
    (from, to, name) <- findLinks(step)
  } yield Link(nodes(from.signature.id), nodes(to.signature.id), name)

  def findNodes(s: HasSignature): Iterable[HasSignature] =
    s.signature.dependencies.flatMap(t => findNodes(t._2))

  def findLinks(s: HasSignature): Iterable[(HasSignature, HasSignature, String)] =
    s.signature.dependencies.map { case (name, dep) => (s, dep, name)} ++
      s.signature.dependencies.flatMap(t => findLinks(t._2))

}

case class Node(id: String, name: String, params: Map[String, String], path: Option[String])

case class Link(from: Node, to: Node, name: String)
