package service.upload

import java.nio.file.Paths
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.FileIO
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait HttpUploadRoute extends StrictLogging {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer

  val conf = ConfigFactory.load()

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

              writeResult.map(result =>
                result.status match {
                  case Success(_) =>
                    val elapsedTime = (System.currentTimeMillis() - startTime).milliseconds
                    logger.info(s"file: $outFileName, time elapsed: ${elapsedTime.toSeconds}")
                    HttpResponse(StatusCodes.OK, entity = s"Successfully written ${result.count} bytes\n")

                  case Failure(e) => errorResponse(e)
                }).recover { case e => errorResponse(e) }
            }
        }
      }
    }
}

object HttpUploadService extends App with HttpUploadRoute {
  override implicit val system = ActorSystem("HttpUploadService")

  override implicit def executor = system.dispatcher

  override implicit val materializer = ActorMaterializer()

  Http().bindAndHandle(uploadFile, interface = "localhost", port = 8080)
}
