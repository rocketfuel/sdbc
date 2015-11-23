organization := "com.rocketfuel.sdbc.jdbc"

name := "scalaz"

description := "Extensions for SDBC's JDBC support for use with Scalaz streaming."

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.scalaz.stream" %% "scalaz-stream" % "0.8"
)

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
