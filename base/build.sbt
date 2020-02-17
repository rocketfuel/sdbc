name := "base"

description := "SDBC is a database API for Scala."

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "2.2.2",
  //Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.1" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.1" % "test",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.1" % "test",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  //Config file loading
  //https://github.com/typesafehub/config
  "com.typesafe" % "config" % "1.3.1",
  "com.chuusai" %% "shapeless" % "2.3.3",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.3" % "test",
  "org.scalaz" %% "scalaz-core" % "7.2.30" % "test",
  "org.apache.commons" % "commons-lang3" % "3.5" % "test"
)

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
