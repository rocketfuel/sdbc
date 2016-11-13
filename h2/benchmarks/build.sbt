publishArtifact in ThisBuild := false

enablePlugins(JmhPlugin)

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core" % "0.3.0",
  "org.tpolecat" %% "doobie-contrib-h2" % "0.3.0",
  "org.slf4j" % "slf4j-simple" % "1.7.21"
)
