organization := "com.rocketfuel.sdbc"

name := "examples"

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut-scalaz" % "6.2-RC2",
  "co.fs2" %% "fs2-io" % "0.9.2"
)

Common.settings

publishArtifact := false

publishArtifact in Test := false
