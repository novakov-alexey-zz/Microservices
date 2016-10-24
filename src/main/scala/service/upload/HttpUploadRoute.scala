package service.upload

import java.nio.file.Paths
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.FileIO
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait HttpUploadRoute extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf: Config

  val uploadFile =
    path("upload" / "csv") {
      post {
        fileUpload("csv") {
          case (metadata, byteSource) =>
            val startTime = System.currentTimeMillis()
            val outFileName = startTime + "_" + metadata.fileName
            logger.info(s"uploading file: $outFileName")

            val sink = FileIO.toPath(Paths.get(conf.getString("upload.output-path")).resolve(outFileName))
            val writeResult = byteSource.runWith(sink)

            complete {
              def errorResponse(e: Throwable) = {
                logger.error(s"Fail on file: $outFileName, start time: ${new Date(startTime)}", e)
                HttpResponse(StatusCodes.InternalServerError, entity = "Error in file uploading\n")
              }

              writeResult.map(r =>
                r.status match {
                  case Success(_) =>
                    val elapsedTime = (System.currentTimeMillis() - startTime).milliseconds
                    logger.info(s"file uploaded: $outFileName, time spent: ${elapsedTime.toSeconds} sec")
                    notifyDataProcessor(outFileName)
                    HttpResponse(StatusCodes.OK, entity = s"Successfully written ${r.count} bytes\n")

                  case Failure(e) => errorResponse(e)
                }).recover { case e => errorResponse(e) }
            }
        }
      }
    }

  def notifyDataProcessor(fileName: String) = {
    val dataProcessorUri = conf.getString("upload.data-processor-uri") + s"/$fileName"
    Http().singleRequest(HttpRequest(uri = dataProcessorUri, method = HttpMethods.POST))
      .onComplete {
        case Success(response) => logger.info(s"data processor response: '$response' for file = $fileName")
        case Failure(e) => logger.error(s"Failed to send request to $dataProcessorUri")
      }
  }
}

object HttpUploadService extends App with HttpUploadRoute {
  implicit val system = ActorSystem("HttpUploadService")

  implicit def executor = system.dispatcher

  implicit val materializer = ActorMaterializer()

  val conf = ConfigFactory.load()

  Http().bindAndHandle(uploadFile, interface = "localhost", port = conf.getInt("upload.http-port"))
}
