import NativePackagerHelper._
import Keys._
name := "kafkainfluxexporter"

version := "0.1"

scalaVersion := "2.12.8"
val http4sVersion = "0.20.0-M4"


libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.1"
libraryDependencies += "com.ovoenergy" %% "fs2-kafka" % "0.18.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.10.1"
libraryDependencies += "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.10.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test


scalacOptions += "-Ypartial-unification"

enablePlugins(JavaAppPackaging)

mappings in Universal += sourceDirectory.value / "main" / "resources" / "application.conf" -> "conf/application.conf"

bashScriptConfigLocation := Some("${app_home}/../conf/jvmopts")
// add jvm parameter for typesafe config
bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf""""
bashScriptExtraDefines += """addJava "-Djava.security.auth.login.config=${app_home}/../conf/jaas.conf""""

mainClass in Compile := Some("com.globalmaksimum.kafkainfluxexporter.Main")

