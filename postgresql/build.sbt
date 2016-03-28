organization := "com.rocketfuel.sdbc"

name := "jdbc-postgresql"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1208.jre7",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
