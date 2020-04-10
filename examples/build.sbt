organization := "com.rocketfuel.sdbc"

name := "examples"

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut-scalaz" % "6.2.5",
  Common.fs2IO(scalaVersion.value)
)

Common.settings

publishArtifact := false

publishArtifact in Test := false
