organization := "com.rocketfuel.sdbc.scalaz"

name := "jdbc"

description := "Extensions for SDBC for use with Scalaz streaming"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "me.jeffshaw.scalaz.stream" %% "iterator" % "3.0.1a"
)
