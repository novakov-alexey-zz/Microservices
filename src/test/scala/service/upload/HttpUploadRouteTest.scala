package service.upload

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, Multipart, StatusCodes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class HttpUploadRouteTest extends WordSpec with Matchers with ScalatestRouteTest with HttpUploadRoute {

  val conf = ConfigFactory.load("test-application.conf")

  implicit def default(implicit system: ActorSystem) = RouteTestTimeout(10.second.dilated)

  "Service" should {
    "return OK when upload file content via multipart" in {
      //given
      val content = "This is test file"
      val fileData = Multipart.FormData.BodyPart.Strict(
        "csv",
        HttpEntity(ContentTypes.`text/plain(UTF-8)`, content),
        Map("fileName" -> "test.txt"))
      val formData = Multipart.FormData(fileData)

      //when
      Post("/upload/csv", formData) ~> uploadFile ~> check {
        //then
        status shouldBe StatusCodes.OK
        responseAs[String] shouldEqual s"Successfully written ${content.length} bytes\n"
      }
    }

    "return OK when upload file from path via multipart" in {
      //given
      val bytes = "2330"
      val testFile = new File("src/test/resources/test.csv")
      val fileData = Multipart.FormData.BodyPart.fromPath("csv", ContentTypes.`text/plain(UTF-8)`, testFile.toPath)
      val formData = Multipart.FormData(fileData)

      //when
      Post("/upload/csv", formData) ~> uploadFile ~> check {
        //then
        status shouldBe StatusCodes.OK
        responseAs[String] shouldEqual s"Successfully written $bytes bytes\n"
      }
    }
  }
}