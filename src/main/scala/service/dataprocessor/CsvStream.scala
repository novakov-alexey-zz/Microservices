package service.dataprocessor

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import service.dataprocessor.RecordTransformation.recordToEvent
import service.dataprocessor.dal.{Event, EventDao}

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

trait CsvStream extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  def startCsvStream(eventDao: EventDao) = {
    val inputFile = "1476729028285_data_test.csv"
    val inputPath = Paths.get(conf.getString("data-processor.input-path")).resolve(inputFile)
    val fileSource = FileIO.fromPath(inputPath)

    val fileSink = FileIO.toPath(Paths.get(conf.getString("data-processor.processed-path")).resolve(inputFile))
    //TODO: move startTime, so that it has to be initialized per each Kafka message processed
    val startTime = System.currentTimeMillis()

    val rowDelim = ByteString("\n")

    fileSource
      .via(Framing.delimiter(rowDelim, Int.MaxValue, allowTruncation = true))
      .fold(mutable.HashMap.empty[ByteString, ByteString])(Deduplication())
      .log("record count", _.keySet.size)
      .log("records", _.foreach { case (key, value) => logger.debug(s"${key.utf8String}->${value.utf8String}") })
      .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
      .via(EventStore(eventDao))
      .runReduce((a, _) => a)
      .onComplete(result => {
        result match {
          case Success(r) =>
          //logger.info(s"result line count: ${r.keySet.size}")
          //r.foreach { case (key, value) => logger.debug(s"${key.utf8String}->${value.utf8String}") }
          case Failure(e) => logger.error("Stream failed.", e)
        }
        //system.terminate()
        logger.info(s"elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} sec") // 82 sec
      })
  }

  object Deduplication {
    val comma = ','.toByte

    type Map = mutable.HashMap[ByteString, ByteString]

    def apply(): (Map, ByteString) => Map = {
      (map, bs) => {
        map += bs.takeWhile(_ != comma) -> bs
      }
    }
  }
}

object EventStore {

  def apply(eventDao: EventDao): Flow[mutable.HashMap[ByteString, ByteString], Unit, NotUsed] =
    Flow.fromFunction(records =>
      records
        .filterKeys(isNumeric)
        .values
        .map(_.utf8String.split(delim))
        .filter(nonEmptyFields)
        .map(recordToEvent)
        .foreach(eventDao.insertEvent)
    )

  val isNumeric: (ByteString) => Boolean = bs => Try(bs.utf8String.toLong).isSuccess

  def nonEmptyFields: (Array[String]) => Boolean = a =>
    a.isDefinedAt(1) && a(1).nonEmpty &&
      a.isDefinedAt(2) && a(2).nonEmpty
}

object RecordTransformation {
  import service.dataprocessor.DateParser._

  def recordToEvent(record: Array[String]): Event = {
    println("to event = " + record.mkString(", "))
    Event(id = record(0).toLong, name = record(1), timeOfStart = record(2))
  }
}

object DataProcessorService extends App with CsvStream {
  override implicit val system = ActorSystem("DataProcessorService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  val eventDao = Modules.injector.getInstance(classOf[EventDao])
  startCsvStream(eventDao)
}
