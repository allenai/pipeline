package org.allenai.pipeline

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.Config
import org.allenai.common.Config._
import org.allenai.common.Logging
import org.allenai.pipeline.IoHelpers._
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** A top-level data flow pipeline.
  * Provides methods for persisting Producers in a consistent location,
  * running Producers,
  * and producing a visualization of the end-to-end data flow
  */
trait Pipeline extends Logging {

  def rootOutputUrl: URI

  /** Run the pipeline.  All steps that have been persisted will be computed, along with any upstream dependencies */
  def run(title: String) = {
    runPipelineReturnResults(title, persistedSteps)
  }

  def persistedSteps = steps.toMap

  /** Common-case methods for persisting Producers */
  object Persist {

    /** Persist a collection */
    object Collection {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterable[T]],
        stepName: String = null,
        suffix: String = ".txt"
      )(): PersistedProducer[Iterable[T], FlatArtifact] =
        persist(step, LineCollectionIo.text[T], stepName, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterable[T]],
        stepName: String = null,
        suffix: String = ".json"
      )(): PersistedProducer[Iterable[T], FlatArtifact] =
        persist(step, LineCollectionIo.json[T], stepName, suffix)
    }

    /** Persist a single object */
    object Singleton {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[T],
        stepName: String = null,
        suffix: String = ".txt"
      )(): PersistedProducer[T, FlatArtifact] =
        persist(step, SingletonIo.text[T], stepName, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[T],
        stepName: String = null,
        suffix: String = ".json"
      )(): PersistedProducer[T, FlatArtifact] =
        persist(step, SingletonIo.json[T], stepName, suffix)
    }

    /** Persist an Iterator */
    object Iterator {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterator[T]],
        stepName: String = null,
        suffix: String = ".txt"
      ): PersistedProducer[Iterator[T], FlatArtifact] =
        persist(step, LineIteratorIo.text[T], stepName, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterator[T]],
        stepName: String = null,
        suffix: String = ".json"
      )(): PersistedProducer[Iterator[T], FlatArtifact] =
        persist(step, LineIteratorIo.json[T], stepName, suffix)
    }

  }

  /** Create a persisted version of the given Producer
    * The producer is registered as a target of the pipeline, and will be computed
    * when the pipeline is run.
    * See Persist.Collection, Persist.Singleton, etc. utility methods above
    * @param original the non-persisted Producer
    * @param io the serialization format
    * @param suffix a file suffix
    * @return the persisted Producer
    */
  def persist[T, A <: Artifact: ClassTag](
    original: Producer[T],
    io: Serializer[T, A] with Deserializer[T, A],
    name: String = null,
    suffix: String = ""
  ): PersistedProducer[T, A] = {
    val stepName = Option(name).getOrElse(original.stepInfo.className)
    val path = s"data/$stepName.${hashId(original, io)}$suffix"
    persistToArtifact(original, io, createOutputArtifact[A](path), name)
  }

  def persistToUrl[T, A <: Artifact: ClassTag](
    original: Producer[T],
    io: Serializer[T, A] with Deserializer[T, A],
    url: URI,
    name: String = null
  ): PersistedProducer[T, A] = {
    persistToArtifact(original, io, artifactFactory.createArtifact[A](url), name)
  }

  /** Persist this Producer and add it to list of targets that will be computed when the pipeline is run */
  def persistToArtifact[T, A <: Artifact](
    original: Producer[T],
    io: Serializer[T, A] with Deserializer[T, A],
    artifact: A,
    name: String = null
  ): PersistedProducer[T, A] = {
    val persisted = new ProducerWithPersistence(original, io, artifact)
    val stepName = Option(name).getOrElse(original.stepInfo.className)
    addTarget(stepName, persisted)
  }

  def persistCustom[T, P <: Producer[T], A <: Artifact: ClassTag](
    original: P,
    makePersisted: (P, A) => PersistedProducer[T, A],
    name: String = null,
    suffix: String = ""
  ): PersistedProducer[T, A] = {
    val stepName = Option(name).getOrElse(original.stepInfo.className)
    val path = s"data/$stepName.${original.stepInfo.signature.id}$suffix"
    val artifact = createOutputArtifact[A](path)
    addTarget(stepName, makePersisted(original, artifact))
  }

  def addTarget[T, A <: Artifact](name: String, target: PersistedProducer[T, A]) = {
    var i = 1
    var stepName = name
    while (steps.contains(stepName)) {
      stepName = s"$name.$i"
      i += 1
    }
    steps(stepName) = target
    target
  }

  protected[this] def urlToArtifact = CreateCoreArtifacts.fromFileUrls

  def artifactFactory = ArtifactFactory(urlToArtifact)

  /** Create an Artifact at the given path, relative to the rootOutputUrl
    * (path may also be an absolute URL)
    */
  def createOutputArtifact[A <: Artifact: ClassTag](path: String): A =
    artifactFactory.createArtifact[A](rootOutputUrl, path)

  /** Use a local file as an input resource.
    * A versioned copy of the file will be stored in the root output location.
    * This allows the pipeline run to be reproducible even in the contents
    * of the input file change subsequently.
    */
  def versionedInputFile(file: File): FlatArtifact = {
    val (fileName, extension) =
      file.getName.split('.') match {
        case Array(s) => (s, "")
        case a =>
          (a.take(a.size - 1).mkString("."), a.last)
      }
    def createVersionedArtifact(id: String) =
      createOutputArtifact[FlatArtifact](s"data/$fileName.$id.$extension")
    new VersionedInputFile(new FileArtifact(file), createVersionedArtifact)
  }

  def getStepsByName(targetNames: Iterable[String]) = {
    val targets = targetNames.flatMap(s => steps.get(s).map(p => (s, p)))
    if (targets.size != targetNames.size) {
      val unresolvedNames = targetNames.filterNot(steps.contains)
      sys.error(s"Step names not found: ${unresolvedNames.mkString("[", ",", "]")}")
    }
    targets
  }

  /** Run only specified steps in the pipeline.  Upstream dependencies must exist already.  They will not be computed */
  def runOnly(title: String, targetNames: String*): Iterable[(String, Any)] = {
    runOnly(title, getStepsByName(targetNames))
  }

  def runOnly(title: String, targets: Iterable[(String, Producer[_])]): Iterable[(String, Any)] = {
    val targetStepInfo = targets.map(_._2.stepInfo).toSet
    val allDependencies = targets.flatMap { case (s, p) => Workflow.upstreamDependencies(p) }
    val nonExistentDependencies =
      for {
        p <- allDependencies if p.isInstanceOf[PersistedProducer[_, _]]
        pp = p.asInstanceOf[PersistedProducer[_, _ <: Artifact]]
        if !targetStepInfo(pp.stepInfo)
        if !pp.artifact.exists
      } yield pp.stepInfo
    require(nonExistentDependencies.isEmpty, {
      val targetNames = targetStepInfo.map(_.className).mkString(",")
      val dependencyNames = nonExistentDependencies.map(_.className).mkString(",")
      s"Cannot run steps [$targetNames]. Upstream dependencies [$dependencyNames] have not been computed"
    })
    runPipelineReturnResults(title, targets)
  }

  protected[this] def runPipelineReturnResults(rawTitle: String, targets: Iterable[(String, Producer[_])]): Iterable[(String, Any)] = {
    // Order the outputs so that the ones with the fewest dependencies are executed first
    val outputs = targets.toVector.map {
      case (name, p) => (Workflow.upstreamDependencies(p).size, name, p)
    }.sortBy(_._1).map { case (count, name, p) => (name, p) }
    val result: Seq[(String, Any)] = try {
      val start = System.currentTimeMillis
      val result = outputs.map { case (name, p) => (name, p.get) }
      val duration = (System.currentTimeMillis - start) / 1000.0
      logger.info(f"Ran pipeline in $duration%.3f s")
      result
    } catch {
      case NonFatal(e) =>
        logger.error("Untrapped exception", e)
        List()
    }

    val title = rawTitle.replaceAll("""\s+""", "-")
    val today = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())

    val workflowArtifact = createOutputArtifact[FlatArtifact](s"summary/$title-$today.workflow.json")
    val workflow = Workflow.forPipeline(targets.toMap)
    SingletonIo.json[Workflow].write(workflow, workflowArtifact)

    val htmlArtifact = createOutputArtifact[FlatArtifact](s"summary/$title-$today.html")
    SingletonIo.text[String].write(workflow.renderHtml, htmlArtifact)

    val signatureArtifact = createOutputArtifact[FlatArtifact](s"summary/$title-$today.signatures.json")
    val signatureFormat = Signature.jsonWriter
    val signatures = targets.map { case (s, p) => signatureFormat.write(p.stepInfo.signature) }.toList.toJson
    signatureArtifact.write { writer => writer.write(signatures.prettyPrint) }

    logger.info(s"Summary written to ${toHttpUrl(htmlArtifact.url)}")
    result
  }

  // Generate a hash unique to this Producer/Serialization combination
  protected def hashId[T, A <: Artifact](
    p: Producer[T],
    io: Serializer[T, A] with Deserializer[T, A]
  ) =
    p.stepInfo.copy(
      dependencies = p.stepInfo.dependencies + ("io" -> io)
    ).signature.id

  // Convert S3 URLs to an http: URL viewable in a browser
  def toHttpUrl(url: URI): URI = {
    url.getScheme match {
      case "s3" | "s3n" =>
        new java.net.URI("http", s"${
          url.getHost
        }.s3.amazonaws.com", url.getPath, null)
      case "file" =>
        new java.net.URI(null, null, url.getPath, null)
      case _ => url
    }
  }

  def dryRun(outputDir: File, rawTitle: String, targets: Iterable[(String, Producer[_])] = persistedSteps.toList): Unit = {
    val title = s"${
      rawTitle.replaceAll("""\s+""", "-")
    }-dryRun"
    val workflowArtifact = new FileArtifact(new File(outputDir, s"$title.workflow.json"))
    val workflow = Workflow.forPipeline(targets)
    SingletonIo.json[Workflow].write(workflow, workflowArtifact)

    val htmlArtifact = new FileArtifact(new File(outputDir, s"$title.html"))
    SingletonIo.text[String].write(workflow.renderHtml, htmlArtifact)

    val signatureArtifact = new FileArtifact(new File(outputDir, s"$title.signatures.json"))
    val signatureFormat = Signature.jsonWriter
    val signatures = targets.map { case (s, p) => signatureFormat.write(p.stepInfo.signature) }.toJson
    signatureArtifact.write {
      writer => writer.write(signatures.prettyPrint)
    }

    logger.info(s"Summary written to $outputDir")
  }

  protected[this] val steps =
    scala.collection.mutable.Map.empty[String, PersistedProducer[_, _ <: Artifact]]
}

object Pipeline {
  // Create a Pipeline that writes output to the given directory
  def apply(rootDir: File) =
    new Pipeline {
      def rootOutputUrl = rootDir.toURI
    }

  def configured(cfg: Config) =
    new ConfiguredPipeline {
      val config = cfg
    }
}

trait ConfiguredPipeline extends Pipeline {
  val config: Config

  override def rootOutputUrl =
    config.get[String]("output.dir").map(s => new URI(s))
      .getOrElse(new File(System.getProperty("user.dir")).toURI)

  protected[this] def getStringList(key: String) =
    config.get[String](key) match {
      case Some(s) => List(s)
      case None => config.get[Seq[String]](key) match {
        case Some(sList) => sList
        case None => List()
      }
    }

  override def run(rawTitle: String) = {
    val (targets, isRunOnly) =
      getStringList("runOnly") match {
        case seq if seq.nonEmpty =>
          (getStepsByName(seq), true)
        case _ =>
          getStringList("runUntil") match {
            case seq if seq.nonEmpty =>
              (getStepsByName(seq), false)
            case _ =>
              (persistedSteps.toList, false)
          }
      }

    config.get[Boolean]("dryRun") match {
      case Some(true) =>
        val outputDir = config.get[String]("dryRunOutput")
          .getOrElse(System.getProperty("user.dir"))
        dryRun(new File(outputDir), rawTitle, targets)
        List()
      case _ =>
        if (isRunOnly) {
          runOnly(rawTitle, targets)
        } else {
          runPipelineReturnResults(rawTitle, targets)
        }
    }
  }

  override def persist[T, A <: Artifact: ClassTag](
    original: Producer[T],
    io: Serializer[T, A] with Deserializer[T, A],
    name: String = null,
    suffix: String = ""
  ): PersistedProducer[T, A] = {
    val stepName = Option(name).getOrElse(original.stepInfo.className)
    val configKey = s"output.persist.$stepName"
    if (config.hasPath(configKey)) {
      config.getValue(configKey).unwrapped() match {
        case path: String if path != "false" =>
          super.persistToArtifact(original, io, createOutputArtifact[A](path), name)
        case java.lang.Boolean.FALSE | "false" =>
          // Disable persistence
          new ProducerWithPersistence(original, io, createOutputArtifact[A]("save-disabled")) {
            override def create = original.get

            override def stepInfo = original.stepInfo
          }
        case _ =>
          super.persist(original, io, name, suffix)
      }
    } else {
      super.persist(original, io, name, suffix)
    }
  }
}

