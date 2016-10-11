organization := "com.rocketfuel.sdbc"

name := "datastax-cassandra"

description := "An implementation of SDBC for accessing Apache Cassandra using the DataStax driver."

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.1",
  "com.chuusai" %% "shapeless" % "2.2.5",
  "com.google.code.findbugs" % "jsr305" % "3.0.1",
  "org.scodec" %% "scodec-bits" % "1.1.2",
  "org.scalaz.stream" %% "scalaz-stream" % "0.8.4",
  "org.cassandraunit" % "cassandra-unit" % "3.0.0.1" % "test"
)

parallelExecution := false
