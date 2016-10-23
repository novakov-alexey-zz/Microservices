package service.dataprocessor

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import service.dataprocessor.dal.{Event, EventDao}

class CsvStreamTest extends FlatSpec with Matchers with MockFactory with ScalaFutures with CsvStream {
  override implicit val system = ActorSystem("DataProcessorServiceTest")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  override val conf = ConfigFactory.load()

  it should "get only unique events from input csv file and store to db" in {
    //given
    val fileName = "test.csv"
    val eventDao = stub[EventDao]
    //when
    val result = startCsvStream(fileName, eventDao)
    whenReady(result) { _ =>
      //then
      eventDao.insertEvent _ verify * repeat 2
    }
  }
}
