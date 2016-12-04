organization := "com.rocketfuel.sdbc.mariadb"

name := "jdbc"

description := "An implementation of SDBC for accessing MariaDB using JDBC."

libraryDependencies ++= Seq(
  "org.mariadb.jdbc" % "mariadb-java-client" % "1.5.5",
  "ch.vorburger.mariaDB4j" % "mariaDB4j" % "2.2.2" % "test"
)

parallelExecution := false
