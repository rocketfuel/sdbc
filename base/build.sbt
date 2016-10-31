organization := "com.rocketfuel.sdbc"

name := "base"

description := "SDBC is a database API for Scala."

val macroParadiseVersion = "2.1.0"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "0.9.1",
  //Logging
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.7" % "test",
  "org.apache.logging.log4j" % "log4j-core" % "2.7" % "test",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  //Config file loading
  //https://github.com/typesafehub/config
  "com.typesafe" % "config" % "1.3.0",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.2" % "test",
  "org.scalaz" %% "scalaz-core" % "7.2.7" % "test",
  "org.apache.commons" % "commons-lang3" % "3.5" % "test"
)

libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 10)) =>
      Seq(
        compilerPlugin("org.scalamacros" % "paradise" % macroParadiseVersion cross CrossVersion.full),
        "org.scalamacros" %% "quasiquotes" % macroParadiseVersion
      )
    case Some((2, _)) =>
      Seq("org.scala-lang.modules" %% "scala-xml" % "1.0.5")
  }
}

crossScalaVersions := Seq("2.10.6")
