package service.dataprocessor

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import service.dataprocessor.dal.EventDao

class CsvStreamTest extends FlatSpec with Matchers with MockFactory with ScalatestRouteTest with CsvStream {

  override val conf: Config = ConfigFactory.load()

  it should "return Ok and accepted message when sending process/csv request" in {
    //given
    val eventDao = stub[EventDao]
    val fileName = "test_file.csv"

    //when
    Post(s"/process/csv/$fileName") ~> csvStream(eventDao) ~> check {
      //then
      status shouldBe StatusCodes.OK
      responseAs[String] shouldEqual s"File accepted $fileName"
    }
  }
}
