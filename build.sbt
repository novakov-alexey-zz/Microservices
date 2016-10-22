name := "Microservices-upload-file"

version := "1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.11"

val scalaLoggingVersion = "3.1.0"
val logbackVersion = "1.1.2"
val loggingScala    = "com.typesafe.scala-logging"  %% "scala-logging"                  % scalaLoggingVersion
val loggingLogback  = "ch.qos.logback"              %  "logback-classic"                % logbackVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
//  "mysql" % "mysql-connector-java" % "5.1.16",
  "com.h2database" % "h2" % "1.4.192",
  "org.mybatis.scala" % "mybatis-scala-core_2.11" % "1.0.3",
  "com.google.inject" % "guice" % "4.0",
  loggingScala,
  loggingLogback,

  "com.typesafe.akka" %% "akka-http-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.3.0" % "test"
)