organization := "com.rocketfuel.sdbc"

name := "examples"

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut-scalaz" % "6.2.4",
  "co.fs2" %% "fs2-io" % "2.2.2"
)

Common.settings

publishArtifact := false

publishArtifact in Test := false
