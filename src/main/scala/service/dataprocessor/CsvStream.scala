package service.dataprocessor

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Framing, Keep, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

trait CsvStream {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  def startCsvStream() = {
    //TODO: take full path from Kafka message maybe or just file name?
    val inputFile = "1476729028285_data_test.csv"
    val inputPath = Paths.get(conf.getString("data-processor.input-path")).resolve(inputFile)
    val fileSource = FileIO.fromPath(inputPath)

    val fileSink = FileIO.toPath(Paths.get(conf.getString("data-processor.processed-path")).resolve(inputFile))
    val startTime = System.currentTimeMillis()

    val delim = ByteString("\n")
    fileSource
      .via(Framing.delimiter(delim, Int.MaxValue, allowTruncation = true))

      //.grouped(5000)
//      .map(_.foldLeft(mutable.HashMap.empty[String, ByteString])((map, bs) => {
//        map += (bs.utf8String.takeWhile(_ != ',') -> bs)
//      }))
      //.mapAsync(8)(map => Future(map))
//      .mapConcat(ls => new scala.collection.immutable.Iterable[ByteString]() {
//        def iterator = ls.values.iterator
//      })
      //.runWith(Sink.foreach(bs => println(bs.utf8String)))
          //.map(bs => bs.utf8String.split(","))
        .runFold(mutable.HashMap.empty[String, ByteString])((map, bs) => {
              map += bs.utf8String.takeWhile(_ != ',') -> bs
        })
      //.toMat(fileSink)(Keep.right).run()
      .onComplete(result => {
        result match {
          case Success(r) =>
            println(s"processed: ${r.keySet.size}, ${r.keySet}")
            r foreach {case (key, value) => println (key + "->" + value.utf8String)}
          case Failure(e) => println(e)
        }
        system.terminate()
        println(s"elapsed time: ${(System.currentTimeMillis() - startTime) / 1000} sec") //107
      })
  }
}

object DataProcessorService extends App with CsvStream {
  override implicit val system = ActorSystem("DataProcessorService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  startCsvStream()
}
