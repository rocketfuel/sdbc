enablePlugins(JmhPlugin)

val doobieVersion = "0.8.8"

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-h2" % doobieVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.24"
)

Common.settings

publishArtifact := false

publishArtifact in Test := false
