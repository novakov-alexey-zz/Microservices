name := "Microservices-upload-file"

version := "1.0"

scalaVersion := "2.11.8"

val scalaLoggingVersion = "3.1.0"
val logbackVersion = "1.1.2"
val loggingScala    = "com.typesafe.scala-logging"  %% "scala-logging"                  % scalaLoggingVersion
val loggingLogback  = "ch.qos.logback"              %  "logback-classic"                % logbackVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.11",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11",
  loggingScala,
  loggingLogback
)