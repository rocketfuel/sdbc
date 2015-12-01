organization := "com.rocketfuel.sdbc"

name := "cassandra-datastax-scalaz"

description := "Extensions for SDBC's DataStax support for use with Scalaz streaming."

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.scalaz.stream" %% "scalaz-stream" % "0.8"
)
