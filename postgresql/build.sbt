import com.typesafe.tools.mima.core._

organization := "com.rocketfuel.sdbc"

name := "postgresql-jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.10.jre7",
  Common.xml,
  "io.argonaut" %% "argonaut" % "6.2.4" % Provided,
  "org.json4s" %% "json4s-jackson" % "3.6.7" % Provided,

  "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.10" % "test"
)

libraryDependencies += {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 11)) =>
      "io.circe" %% "circe-parser" % "0.11.2" % Provided
    case Some((2, 12 | 13)) =>
      "io.circe" %% "circe-parser" % "0.13.0" % Provided
  }
}

parallelExecution := false

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
