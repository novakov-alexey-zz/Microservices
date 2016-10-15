package service.upload

import java.io.FileOutputStream
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, Multipart, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Flow}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

object HttpUpload extends App with StrictLogging {
  implicit val system = ActorSystem()
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val conf = ConfigFactory.load()

  val uploadFile =
    path("upload" / "csv") {
      post {
        fileUpload("csv") {
          case (metadata, byteSource) =>
            val outFileName = System.currentTimeMillis() + "_" + metadata.fileName
            val sink = FileIO.toPath(Paths.get(conf.getString("upload.output-path")).resolve(outFileName))
            val writeResult = byteSource.runWith(sink)
            onSuccess(writeResult) { result =>
              result.status match {
                case Success(_) => complete(s"Successfully written ${result.count} bytes\n")
                case Failure(e) => complete(HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading\n"))
              }
            }
        }
      }
    }

  def uploadFile2 = {
    path("upload" / "csv") {
      (post & entity(as[Multipart.FormData])) { fileData =>
        complete {
          val outFileName = System.currentTimeMillis() + ".csv"
          val filePath = Paths.get(conf.getString("upload.output-path")).resolve(outFileName).toString
          processFile(filePath, fileData).map { fileSize =>
            HttpResponse(StatusCodes.OK, entity = s"File successfully uploaded. File size is $fileSize\n")
          }.recover {
            case ex: Exception => HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading\n")
          }
        }
      }
    }
  }

  private def processFile(filePath: String, fileData: Multipart.FormData): Future[Int] = {
    val fileOutput = new FileOutputStream(filePath)

    val source = fileData.parts.mapAsync(4) { bodyPart ⇒
      def writeFile(count: Int, byteString: ByteString) = {
        val byteArray = byteString.toArray
        fileOutput.write(byteArray)
        count + byteArray.length
      }
      bodyPart.entity.dataBytes.runFold(0)(writeFile)
    }
    source.runFold(0)(_ + _)
  }

  Http().bindAndHandle(uploadFile, interface = "localhost", port = 8080)
}
