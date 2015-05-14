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

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** A fully-configured end-to-end pipeline */
class Pipeline extends Logging {

  val awsCredentials = S3Artifact.environmentCredentials _

  protected[this] def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] =
    ArtifactFactory.fromAbsoluteUrl[A](awsCredentials)

  def createArtifact[A <: Artifact: ClassTag](path: String): A = {
    val fn = tryCreateArtifact[A]
    ArtifactFactory(fn)(path)
  }

  protected[this] val persistedSteps: ListBuffer[Producer[_]] = ListBuffer()

  /** Run the pipeline.  All steps that have been persisted will be computed, along with any upstream dependencies */
  def run(title: String) = {
    runPipelineReturnResults(title, persistedSteps.toSeq)
  }

  def runOne[T, A <: Artifact: ClassTag](target: PersistedProducer[T, A], outputLocationOverride: Option[String] = None) = {
    val targetWithOverriddenLocation: Producer[T] =
      outputLocationOverride match {
        case Some(tmp) =>
          target.changePersistence(target.io, createArtifact[A](tmp))
        case None => target
      }
    runOnly(
      targetWithOverriddenLocation.stepInfo.className,
      List(targetWithOverriddenLocation): _*
    ).head.asInstanceOf[T]
  }

  /** Run only specified steps in the pipeline.  Upstream dependencies must exist already.  They will not be computed */
  def runOnly(title: String, runOnlyTargets: Producer[_]*) = {
    val (persistedTargets, unpersistedTargets) = runOnlyTargets.partition(_.isInstanceOf[PersistedProducer[_, _]])
    val targets = persistedTargets ++
      unpersistedTargets.flatMap(s => persistedSteps.find(_.stepInfo.signature == s.stepInfo.signature))
    require(targets.size == runOnlyTargets.size, "Specified targets are not members of this pipeline")

    val persistedStepsInfo = (persistedSteps ++ persistedTargets).map(_.stepInfo).toSet
    val overridenStepsInfo = targets.map(_.stepInfo).toSet
    val nonPersistedTargets = overridenStepsInfo -- persistedStepsInfo
    require(
      nonPersistedTargets.size == 0,
      s"Running a pipeline without persisting the output: [${nonPersistedTargets.map(_.className).mkString(",")}]"
    )
    val allDependencies = targets.flatMap(Workflow.upstreamDependencies)
    val nonExistentDependencies =
      for {
        p <- allDependencies if p.isInstanceOf[PersistedProducer[_, _]]
        pp = p.asInstanceOf[PersistedProducer[_, _ <: Artifact]]
        if !overridenStepsInfo(pp.stepInfo)
        if !pp.artifact.exists
      } yield pp.stepInfo
    require(nonExistentDependencies.size == 0, {
      val targetNames = overridenStepsInfo.map(_.className).mkString(",")
      val dependencyNames = nonExistentDependencies.map(_.className).mkString(",")
      s"Cannot run steps [$targetNames]. Upstream dependencies [$dependencyNames] have not been computed"
    })
    runPipelineReturnResults(title, targets)
  }

  private def runPipelineReturnResults(rawTitle: String, outputs: Iterable[Producer[_]]) = {
    val result = try {
      val start = System.currentTimeMillis
      val result = outputs.map(_.get)
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

    val workflowArtifact = createArtifact[FlatArtifact](s"summary/$title-$today.workflow.json")
    val workflow = Workflow.forPipeline(outputs.toSeq: _*)
    SingletonIo.json[Workflow].write(workflow, workflowArtifact)

    val htmlArtifact = createArtifact[FlatArtifact](s"summary/$title-$today.html")
    SingletonIo.text[String].write(workflow.renderHtml, htmlArtifact)

    val signatureArtifact = createArtifact[FlatArtifact](s"summary/$title-$today.signatures.json")
    val signatureFormat = Signature.jsonWriter
    val signatures = outputs.map(p => signatureFormat.write(p.stepInfo.signature)).toList.toJson
    signatureArtifact.write { writer => writer.write(signatures.prettyPrint) }

    logger.info(s"Summary written to ${toHttpUrl(htmlArtifact.url)}")
    result
  }

  def persist[T, A <: Artifact: ClassTag, AO <: A](
    original: Producer[T],
    io: ArtifactIo[T, A],
    fileName: Option[String] = None,
    suffix: String = "",
    makeArtifactOverride: PartialFunction[String, AO] = PartialFunction.empty
  ): PersistedProducer[T, A] = {
    val path = fileName.map(s =>
      if (new URI(s).getScheme != null) {
        s
      } else {
        s"data/$s"
      })
      .getOrElse(s"data/${autoGeneratedPath(original, io)}")
    val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
    val artifact = makeArtifactOverride.applyOrElse(s"$path$suffix", createArtifact[A])
    persist(original, io, artifact)
  }

  def persist[T, A <: Artifact](
    original: Producer[T],
    io: ArtifactIo[T, A],
    artifact: A
  ) = {
    val persisted = original.persisted(io, artifact)
    persistedSteps += persisted
    persisted
  }

  // Generate an output path based on the Producer's signature
  protected def autoGeneratedPath[T, A <: Artifact](p: Producer[T], io: ArtifactIo[T, A]) = {
    // Although the persistence method does not affect the signature
    // (the same object will be returned in all cases), it is used
    // to determine the output path, to avoid parsing incompatible data
    val signature = p.stepInfo.copy(
      dependencies = p.stepInfo.dependencies + ("io" -> io)
    ).signature
    s"${
      signature.name
    }.${
      signature.id
    }"
  }

  object Persist {

    object Iterator {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterator[T]],
        path: Option[String] = None,
        suffix: String = ""
      ): PersistedProducer[Iterator[T], FlatArtifact] =
        persist(step, LineIteratorIo.text[T], path, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterator[T]],
        path: Option[String] = None,
        suffix: String = ""
      )(): PersistedProducer[Iterator[T], FlatArtifact] =
        persist(step, LineIteratorIo.json[T], path, suffix)
    }

    object Collection {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterable[T]],
        path: Option[String] = None,
        suffix: String = ""
      )(): PersistedProducer[Iterable[T], FlatArtifact] =
        persist(step, LineCollectionIo.text[T], path, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterable[T]],
        path: Option[String] = None,
        suffix: String = ""
      )(): PersistedProducer[Iterable[T], FlatArtifact] =
        persist(step, LineCollectionIo.json[T], path, suffix)
    }

    object Singleton {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[T],
        path: Option[String] = None,
        suffix: String = ""
      )(): PersistedProducer[T, FlatArtifact] =
        persist(step, SingletonIo.text[T], path, suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[T],
        path: Option[String] = None,
        suffix: String = ""
      )(): PersistedProducer[T, FlatArtifact] =
        persist(step, SingletonIo.json[T], path, suffix)
    }

  }

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

  def dryRun(outputDir: File, rawTitle: String): Iterable[Any] = {
    val outputs = persistedSteps.toList
    val title = s"${
      rawTitle.replaceAll("""\s+""", "-")
    }-dryRun"
    val workflowArtifact = new FileArtifact(new File(outputDir, s"$title.workflow.json"))
    val workflow = Workflow.forPipeline(outputs: _*)
    SingletonIo.json[Workflow].write(workflow, workflowArtifact)

    val htmlArtifact = new FileArtifact(new File(outputDir, s"$title.html"))
    SingletonIo.text[String].write(workflow.renderHtml, htmlArtifact)

    val signatureArtifact = new FileArtifact(new File(outputDir, s"$title.signatures.json"))
    val signatureFormat = Signature.jsonWriter
    val signatures = outputs.map(p => signatureFormat.write(p.stepInfo.signature)).toList.toJson
    signatureArtifact.write {
      writer => writer.write(signatures.prettyPrint)
    }

    logger.info(s"Summary written to $outputDir")
    List()
  }
}

object Pipeline {
  def saveToFileSystem(rootDir: File) = new Pipeline {
    override def tryCreateArtifact[A <: Artifact: ClassTag] =
      CreateFileArtifact.flatRelative[A](rootDir) orElse
        CreateFileArtifact.structuredRelative[A](rootDir) orElse
        super.tryCreateArtifact[A]
  }
  def saveToS3(cfg: S3Config, rootPath: String) = new Pipeline {
    override def tryCreateArtifact[A <: Artifact: ClassTag] =
      CreateS3Artifact.flatRelative[A](cfg, rootPath) orElse
        CreateS3Artifact.structuredRelative[A](cfg, rootPath) orElse
        super.tryCreateArtifact[A]
  }
}

class ConfiguredPipeline(val config: Config) extends Pipeline {

  override protected[this] def tryCreateArtifact[A <: Artifact: ClassTag]: PartialFunction[String, A] = {
    val createRelativeArtifact: PartialFunction[String, A] =
      config.get[String]("output.dir") match {
        case Some(s) => ArtifactFactory.fromRelativeUrl(s, awsCredentials)
        case None => PartialFunction.empty
      }
    createRelativeArtifact orElse super.tryCreateArtifact[A]
  }

  protected[this] val persistedStepsByConfigKey =
    scala.collection.mutable.Map.empty[String, Producer[_]]

  private lazy val runOnlySteps = config.get[String]("runOnly").map(_.split(",").toSet).getOrElse(Set.empty[String])
  protected[this] def tmpOutputOverride[A <: Artifact: ClassTag](stepName: String) = {
    if (isRunOnlyStep(stepName)) {
      val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      config.get[String]("tmpOutput").map { s =>
        ArtifactFactory.fromRelativeUrl[A](s, awsCredentials)
      }
        .getOrElse(PartialFunction.empty)
    } else {
      PartialFunction.empty
    }
  }

  def isRunOnlyStep(stepName: String) = runOnlySteps(stepName)

  override def run(rawTitle: String) = {
    config.get[Boolean]("dryRun") match {
      case Some(true) => dryRun(new File(System.getProperty("user.dir")), rawTitle)
      case _ =>
        config.get[String]("runOnly") match {
          case Some(stepConfigKeys) =>
            val (matchedNames, unmatchedNames) = runOnlySteps.partition(persistedStepsByConfigKey.contains)
            unmatchedNames.size match {
              case 0 =>
                val matches = matchedNames.map(persistedStepsByConfigKey)
                runOnly(rawTitle, matches.toList: _*)
              case 1 =>
                sys.error(s"Unknown step name: ${unmatchedNames.head}")
              case _ =>
                sys.error(s"Unknown step names: [${unmatchedNames.mkString(",")}]")
            }
          case _ => super.run(rawTitle)
        }
    }
  }

  def optionallyPersist[T, A <: Artifact: ClassTag](
    original: Producer[T],
    stepName: String,
    io: ArtifactIo[T, A],
    suffix: String = ""
  ): Producer[T] = {
    val configKey = s"output.persist.$stepName"
    if (config.hasPath(configKey)) {
      config.getValue(configKey).unwrapped() match {
        case java.lang.Boolean.TRUE | "true" =>
          val p = persist(original, io, None, suffix, tmpOutputOverride(stepName))
          persistedStepsByConfigKey(stepName) = p
          p
        case path: String if path != "false" =>
          val p = persist(original, io, Some(path), suffix, tmpOutputOverride(stepName))
          persistedStepsByConfigKey(stepName) = p
          p
        case _ => original
      }
    } else {
      original
    }
  }

  object OptionallyPersist {

    object Iterator {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterator[T]],
        stepName: String,
        suffix: String = ""
      ): Producer[Iterator[T]] =
        optionallyPersist(step, stepName, LineIteratorIo.text[T], suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterator[T]],
        stepName: String,
        suffix: String = ""
      ): Producer[Iterator[T]] =
        optionallyPersist(step, stepName, LineIteratorIo.json[T], suffix)
    }

    object Collection {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[Iterable[T]],
        stepName: String,
        suffix: String = ""
      )(): Producer[Iterable[T]] =
        optionallyPersist(step, stepName, LineCollectionIo.text[T], suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[Iterable[T]],
        stepName: String,
        suffix: String = ""
      )(): Producer[Iterable[T]] =
        optionallyPersist(step, stepName, LineCollectionIo.json[T], suffix)
    }

    object Singleton {
      def asText[T: StringSerializable: ClassTag](
        step: Producer[T],
        stepName: String,
        suffix: String = ""
      )(): Producer[T] =
        optionallyPersist(step, stepName, SingletonIo.text[T], suffix)

      def asJson[T: JsonFormat: ClassTag](
        step: Producer[T],
        stepName: String,
        suffix: String = ""
      )(): Producer[T] =
        optionallyPersist(step, stepName, SingletonIo.json[T], suffix)
    }

  }

}
