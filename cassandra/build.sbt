organization := "com.rocketfuel.sdbc"

name := "cassandra-datastax"

description := "An implementation of SDBC for accessing Apache Cassandra using the DataStax driver."

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.3",
  "com.google.code.findbugs" % "jsr305" % "3.0.1",
  "org.scodec" %% "scodec-bits" % "1.1.4",
  "com.google.guava" % "guava" % "19.0",
  "org.cassandraunit" % "cassandra-unit" % "3.1.1.0" % "test"
)

parallelExecution := false
