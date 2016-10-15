package service.upload

import java.io.FileOutputStream
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, Multipart, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

object HttpUpload extends App {
  implicit val system = ActorSystem()
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val conf = ConfigFactory.load()

  val uploadFile =
    path("fileUpload") {
      post {
        fileUpload("csv") {
          case (metadata, byteSource) =>
            val outFileName = System.currentTimeMillis() + "_" + metadata.fileName
            val sink = FileIO.toPath(Paths.get(conf.getString("upload.output-path")).resolve(outFileName))
            val writeResult = byteSource.runWith(sink)
            onSuccess(writeResult) { result =>
              result.status match {
                case Success(_) => complete(s"Successfully written ${result.count} bytes")
                case Failure(e) => complete(HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading"))
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
            HttpResponse(StatusCodes.OK, entity = s"File successfully uploaded. Fil size is $fileSize")
          }.recover {
            case ex: Exception => HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading")
          }
        }
      }
    }
  }

  private def processFile(filePath: String, fileData: Multipart.FormData): Future[Int] = {
    val fileOutput = new FileOutputStream(filePath)
    val source = fileData.parts.mapAsync(1) { bodyPart ⇒
      def writeFileOnLocal(array: Array[Byte], byteString: ByteString): Array[Byte] = {
        val byteArray: Array[Byte] = byteString.toArray
        fileOutput.write(byteArray)
        array ++ byteArray
      }
      bodyPart.entity.dataBytes.runFold(Array[Byte]())(writeFileOnLocal)
    }
    source.runFold(0)(_ + _.length)
  }

  Http().bindAndHandle(uploadFile2, interface = "localhost", port = 8080)
}
