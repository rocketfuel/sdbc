import com.typesafe.tools.mima.core._

organization := "com.rocketfuel.sdbc"

name := "cassandra-datastax"

description := "An implementation of SDBC for accessing Apache Cassandra using the DataStax driver."

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0",
//  "com.google.code.findbugs" % "jsr305" % "3.0.1",
  "org.scodec" %% "scodec-bits" % "1.1.13",
//  "com.google.guava" % "guava" % "19.0",
  "org.cassandraunit" % "cassandra-unit" % "4.3.1.0" % "test"
)

parallelExecution := false

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
