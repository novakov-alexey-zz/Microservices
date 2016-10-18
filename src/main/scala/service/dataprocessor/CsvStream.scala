package service.dataprocessor

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, FileIO, Flow, Framing, GraphDSL, Merge}
import akka.stream.{ActorMaterializer, FlowShape, Graph, Materializer}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

trait CsvStream extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  def startCsvStream() = {
    //TODO: take file name from Kafka message
    val inputFile = "1476729028285_data_test.csv"
    val inputPath = Paths.get(conf.getString("data-processor.input-path")).resolve(inputFile)
    val fileSource = FileIO.fromPath(inputPath)

    val fileSink = FileIO.toPath(Paths.get(conf.getString("data-processor.processed-path")).resolve(inputFile))
    //TODO: move startTime, so that it has to be initialized per each Kafka message processed
    val startTime = System.currentTimeMillis()

    val rowDelim = ByteString("\n")
    val comma = ','.toByte

    fileSource
      .via(Framing.delimiter(rowDelim, Int.MaxValue, allowTruncation = true))
      .runFold(mutable.HashMap.empty[ByteString, ByteString])((map, bs) => {
        map += bs.takeWhile(_ != comma) -> bs
      })
      .onComplete(result => {
        result match {
          case Success(r) =>
            logger.info(s"result line count: ${r.keySet.size}")
            r.foreach { case (key, value) => logger.debug(s"${key.utf8String}->${value.utf8String}") }
          case Failure(e) => logger.error("Stream failed.", e)
        }
        system.terminate()
        logger.info(s"elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} sec") // 82 sec
      })
  }
}

object WorkerPool {
  def apply[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Graph[FlowShape[In, Out], NotUsed] = {

    GraphDSL.create() { implicit b =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val balance = b.add(Balance[In](workerCount))
      val resultsMerge = b.add(Merge[Out](workerCount))

      for (i <- 0 until workerCount)
        balance.out(i) ~> worker ~> resultsMerge.in(i)

      FlowShape(balance.in, resultsMerge.out)
    }
  }
}


object DataProcessorService extends App with CsvStream {
  override implicit val system = ActorSystem("DataProcessorService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  startCsvStream()
}
