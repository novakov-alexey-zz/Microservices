package service.dataprocessor

import java.nio.file._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import service.dataprocessor.RecordTransformation.recordToEvent
import service.dataprocessor.dal.{Event, EventDao}

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

trait CsvStream extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  val rowDelim = ByteString("\n")

  def csvStream(eventDao: EventDao) =
    path("process" / "csv" / Segment) { (fileName) =>
      post {
        Future {
          val startTime = System.currentTimeMillis()
          val inputPath = Paths.get(conf.getString("data-processor.input-path")).resolve(fileName)
          logger.info(s"Start processing of file $inputPath, size = ${inputPath.toFile.length} bytes")

          val fileSource = FileIO.fromPath(inputPath)
          val result = fileSource
            .via(Framing.delimiter(rowDelim, Int.MaxValue, allowTruncation = true))
            .fold(mutable.HashMap.empty[ByteString, ByteString])(Deduplication())
            .log("records count", _.keySet.size)
            .log("records", _.map { case (key, value) => s"${key.utf8String}->${value.utf8String}\n" })
            .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
            .via(EventStorage(eventDao, fileName))
            .runReduce((a, _) => a) //TODO: replace Reduce with TO

          result.map { _ =>
            //logger.info(s"result line count: ${r.keySet.size}")
            //r.foreach { case (key, value) => logger.debug(s"${key.utf8String}->${value.utf8String}") }
            moveFile(fileName, inputPath)
          }.recover { case e =>
            logger.error(s"Stream failed. Input file: $fileName", e)
          }
        }

        complete {
          HttpResponse(StatusCodes.OK, entity = s"File accepted $fileName")
        }
      }
    }

  def moveFile(inputFile: String, inputPath: Path) = {
    val outPath = Paths.get(conf.getString("data-processor.processed-path")).resolve(inputFile)
    Files.move(inputPath, outPath, StandardCopyOption.ATOMIC_MOVE)
    logger.debug(s"File moved: $inputFile -> $outPath")
  }

  object Deduplication {
    val comma = ','.toByte

    type Map = mutable.HashMap[ByteString, ByteString]

    def apply(): (Map, ByteString) => Map = (map, bs) => {
      map += bs.takeWhile(_ != comma) -> bs
    }
  }

}

object EventStorage {

  def apply(eventDao: EventDao, fileName: String): Flow[mutable.HashMap[ByteString, ByteString], Unit, NotUsed] =
    Flow.fromFunction(records =>
      records
        .filterKeys(isNumeric)
        .values
        .map(_.utf8String.split(delim))
        .filter(nonEmptyFields)
        .map(recordToEvent(_, fileName))
        .foreach(eventDao.insertEvent)
    )

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

object DataProcessorService extends App with CsvStream {
  override implicit val system = ActorSystem("DataProcessorService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  val eventDao = Modules.injector.getInstance(classOf[EventDao])

  Http().bindAndHandle(csvStream(eventDao), interface = "localhost", port = 8081)
}
