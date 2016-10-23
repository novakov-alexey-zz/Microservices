package service

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import akka.util.ByteString

import scala.collection.mutable
import scala.util.Try

package object dataprocessor {
  type MutableMap = mutable.HashMap[ByteString, ByteString]

  val comma = ','

  object DateParser {
    val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy[ [HH][:mm][:ss]]")
    val shortFormatter = DateTimeFormatter.ofPattern("dd-M-yy[ [HH][:mm][:ss]]")

    implicit def str2date(str: String): LocalDateTime = {
      def parseBySecondFormat = Try(LocalDateTime.parse(str, shortFormatter)).toOption

      def defaultDate = LocalDateTime.now()

      val maybeDate = Try(LocalDateTime.parse(str, formatter)).toOption

      maybeDate
        .orElse(parseBySecondFormat)
        .getOrElse(defaultDate)
        .atZone(ZoneId.systemDefault())
        .withZoneSameInstant(ZoneOffset.UTC)
        .toLocalDateTime
    }
  }

}
