organization := "com.rocketfuel.sdbc.cassandra"

name := "datastax"

description := "An implementation of SDBC for accessing Apache Cassandra using the DataStax driver."

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7.1"
    exclude("com.google.guava", "guava"),
  "com.google.guava" % "guava" % "18.0",
  "com.chuusai" %% "shapeless" % "2.2.5",
  "com.google.code.findbugs" % "jsr305" % "3.0.0",
  "org.scodec" %% "scodec-bits" % "1.0.10",
  "org.cassandraunit" % "cassandra-unit" % "2.1.9.2" % "test"
)

parallelExecution := false
