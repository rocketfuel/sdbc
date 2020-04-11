organization := "com.rocketfuel.sdbc"

name := "jdbc"

libraryDependencies ++= Seq(
  //Connection pooling
  "com.zaxxer" % "HikariCP" % "3.4.2",
  Common.scodec(scalaVersion.value),
  Common.xml % Provided,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

Common.settings

mimaPreviousArtifacts := {
  for (previousVersion <- Common.previousVersions) yield {
    organization.value %% name.value % previousVersion
  }
}.toSet
