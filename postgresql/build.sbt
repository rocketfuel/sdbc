import com.typesafe.tools.mima.core._

organization := "com.rocketfuel.sdbc"

name := "postgresql-jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.0.0",
  "io.argonaut" %% "argonaut" % "6.2.4",
  Common.xml,
  "org.json4s" %% "json4s-jackson" % "3.6.7" % Provided,
  "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.9" % "test"
)

parallelExecution := false

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
