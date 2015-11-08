organization := "com.rocketfuel.sdbc.cassandra"

name := "datastax.scalaz"

description := "Extensions for SDBC's DataStax support for use with Scalaz streaming."

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "me.jeffshaw.scalaz.stream" %% "iterator" % "3.0.1a"
)
