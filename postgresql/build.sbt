organization := "com.rocketfuel.sdbc.postgresql"

name := "jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1212",
  "org.json4s" %% "json4s-jackson" % "3.4.2"
)

parallelExecution := false

crossScalaVersions := Seq("2.10.6")
