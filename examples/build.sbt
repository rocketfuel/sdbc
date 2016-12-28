organization := "com.rocketfuel.sdbc"

name := "examples"

publishArtifact := false

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut" % "6.2-RC2",
  "co.fs2" %% "fs2-io" % "0.9.2"
)
