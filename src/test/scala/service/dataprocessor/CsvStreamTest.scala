package service.dataprocessor

import java.nio.file.{Files, Paths, StandardCopyOption}

import akka.actor.ActorSystem
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.stream.ActorMaterializer
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}
import service.dataprocessor.dal.EventDao

import scala.concurrent.duration._

class CsvStreamTest extends FlatSpec with Matchers with MockFactory with ScalaFutures with CsvStream {
  override implicit val system = ActorSystem("DataProcessorServiceTest")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(10.second.dilated)

  override val conf = ConfigFactory.load()

  it should "get only unique events from input csv file and store to db" in {
    //given
    val dataDir = "target/data"
    val fileName = "test.csv"
    val eventDao = stub[EventDao]

    Paths.get(dataDir).toFile.mkdirs()
    Files.copy(Paths.get("src/test/resources", fileName), Paths.get(dataDir, fileName), StandardCopyOption.REPLACE_EXISTING)

    //when
    val result = startCsvStream(fileName, eventDao)
    whenReady(result) { _ =>
      //then
      eventDao.insertEvent _ verify * repeat 2
    }
  }
}
