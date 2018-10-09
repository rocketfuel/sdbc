organization := "com.rocketfuel.sdbc"

name := "postgresql-jdbc"

description := "An implementation of SDBC for accessing PostgreSQL using JDBC."

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.0.0",
  "io.argonaut" %% "argonaut" % "6.2",
  Common.xml,
  "org.json4s" %% "json4s-jackson" % "3.5.0" % Provided,
  "ru.yandex.qatools.embed" % "postgresql-embedded" % "2.9" % "test"
)

parallelExecution := false

Common.settings
