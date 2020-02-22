import com.typesafe.tools.mima.core._

organization := "com.rocketfuel.sdbc"

name := "mariadb-jdbc"

description := "An implementation of SDBC for accessing MariaDB using JDBC."

libraryDependencies ++= Seq(
  "org.mariadb.jdbc" % "mariadb-java-client" % "2.5.4",
  "ch.vorburger.mariaDB4j" % "mariaDB4j" % "2.4.0" % "test"
)

parallelExecution := false

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
