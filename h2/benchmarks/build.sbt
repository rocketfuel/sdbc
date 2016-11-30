publishArtifact in ThisBuild := false

enablePlugins(JmhPlugin)

val doobieVersion = "0.3.1-M2"

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-h2" % doobieVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.21"
)
