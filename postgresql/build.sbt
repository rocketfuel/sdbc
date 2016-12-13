organization := "com.rocketfuel.sdbc"

name := "postgresql-jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1212",
  "org.json4s" %% "json4s-jackson" % "3.5.0",
  "ru.yandex.qatools.embed" % "postgresql-embedded" % "1.16" % "test"
)

parallelExecution := false
