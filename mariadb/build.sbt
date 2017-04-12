organization := "com.rocketfuel.sdbc"

name := "mariadb-jdbc"

description := "An implementation of SDBC for accessing MariaDB using JDBC."

libraryDependencies ++= Seq(
  "org.mariadb.jdbc" % "mariadb-java-client" % "1.5.9",
  "ch.vorburger.mariaDB4j" % "mariaDB4j" % "2.2.3" % "test"
)

parallelExecution := false

Common.settings
