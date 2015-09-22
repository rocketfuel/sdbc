organization := "com.rocketfuel.sdbc.postgresql"

name := "jdbc-java7"

description := "An implementation of WDA SDBC for accessing PostgreSQL."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4-1203-jdbc41",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
