organization := "com.rocketfuel.sdbc.postgresql"

name := "jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4-1204-jdbc42",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
