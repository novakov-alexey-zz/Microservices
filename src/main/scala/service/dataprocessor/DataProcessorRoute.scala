package service.dataprocessor

import java.nio.file._

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import service.dataprocessor.RecordTransformation.recordToEvent
import service.dataprocessor.dal.{Event, EventDao}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

trait DataProcessorRoute extends StrictLogging {
  val conf: Config

  implicit def executor: ExecutionContextExecutor

  def startCsvStream(fileName: String, eventDao: EventDao): Future[Unit]

  def csvStream(eventDao: EventDao) =
    path("process" / "csv" / Segment) { (fileName) =>
      post {
        val result = startCsvStream(fileName, eventDao)
        result.foreach(_ => logger.info(s"data processing has been completed. File: $fileName"))
        result.failed.foreach(e => logger.error("data processing stream failed", e))

        complete {
          HttpResponse(StatusCodes.OK, entity = s"File accepted: $fileName")
        }
      }
    }
}

trait CsvStream extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  val rowDelim = ByteString("\n")

  def emptyMap: MutableMap = mutable.HashMap.empty[ByteString, ByteString]

  def startCsvStream(fileName: String, eventDao: EventDao): Future[Unit] = {
    val startTime = System.currentTimeMillis()
    val inputPath = Paths.get(conf.getString("data-processor.input-path")).resolve(fileName)
    logger.info(s"Start processing of file $inputPath, size = ${inputPath.toFile.length} bytes")

    val fileSource = FileIO.fromPath(inputPath)
    val result = fileSource
      .via(Framing.delimiter(rowDelim, Int.MaxValue, allowTruncation = true))
      .fold(emptyMap)(Deduplication())
      .runWith(EventStorage(eventDao, fileName))

    result.map { r =>
      logResult(r)
      moveFile(fileName, inputPath)
      logger.debug(s"File: $fileName. Spent time: ${(System.currentTimeMillis() - startTime).milliseconds.toSeconds} sec")
    }.recover { case e =>
      logger.error(s"Stream failed. Input file: $fileName", e)
    }
  }

  def logResult(l: List[Event]): Unit = {
    logger.info(s"result event count: ${l.length}")
    l.foreach(e => logger.debug(s"$e"))
  }

  def moveFile(inputFile: String, inputPath: Path) = {
    val outPath = Paths.get(conf.getString("data-processor.processed-path")).resolve(inputFile)
    val dirsCreated = outPath.getParent.toFile.mkdirs()
    logger.debug(s"processed dirs were created: $dirsCreated")
    Files.move(inputPath, outPath, StandardCopyOption.ATOMIC_MOVE)
    logger.debug(s"File moved: $inputFile -> $outPath")
  }

  object Deduplication {
    val delim = comma.toByte

    def apply(): (MutableMap, ByteString) => MutableMap = (map, bs) => {
      map += bs.takeWhile(_ != delim) -> bs
    }
  }

}

object EventStorage {

  def apply(eventDao: EventDao, fileName: String): Sink[MutableMap, Future[List[Event]]] = {
    Sink.fold(List[Event]())((acc, records) => {

      val events = for (
        event <- records
          .filterKeys(isNumeric)
          .values
          .map(_.utf8String.split(comma))
          .filter(nonEmptyFields)
          .map(recordToEvent(_, fileName)))
        yield {
          eventDao.insertEvent(event)
          event
        }

      acc ++ events
    })
  }

  val isNumeric: (ByteString) => Boolean = bs => Try(bs.utf8String.toLong).isSuccess

  def nonEmptyFields: (Array[String]) => Boolean = a =>
    a.isDefinedAt(1) && a(1).nonEmpty &&
      a.isDefinedAt(2) && a(2).nonEmpty
}

object RecordTransformation {

  import service.dataprocessor.DateParser._

  def recordToEvent(record: Array[String], fileName: String) =
    Event(id = record(0).toLong, name = record(1), timeOfStart = record(2), fileName)
}

object DataProcessorService extends App with DataProcessorRoute with CsvStream {
  override implicit val system = ActorSystem("DataProcessorService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  val eventDao = Modules.injector.getInstance(classOf[EventDao])

  Http().bindAndHandle(csvStream(eventDao), interface = "localhost", port = conf.getInt("data-processor.http-port"))
}
