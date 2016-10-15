package service.upload

import java.io.FileOutputStream
import java.nio.file.Paths
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HttpUpload extends App with StrictLogging {
  implicit val system = ActorSystem("HttpUploadService")
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val conf = ConfigFactory.load()

  val uploadFile =
    path("upload" / "csv") {
      post {
        fileUpload("csv") {
          case (metadata, byteSource) =>
            val startTime = System.currentTimeMillis()
            val outFileName = startTime + "_" + metadata.fileName
            val sink = FileIO.toPath(Paths.get(conf.getString("upload.output-path")).resolve(outFileName))
            val writeResult = byteSource.runWith(sink)

            onSuccess(writeResult) { result =>
              result.status match {
                case Success(_) =>
                  val elapsedTime = (System.currentTimeMillis() - startTime).milliseconds
                  logger.info(s"file: $outFileName, time elapsed: ${elapsedTime.toSeconds}")
                  complete(s"Successfully written ${result.count} bytes\n")

                case Failure(e) =>
                  logger.error(s"Fail on file: $outFileName, start time: ${new Date(startTime)}", e)
                  complete(HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading\n"))
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
