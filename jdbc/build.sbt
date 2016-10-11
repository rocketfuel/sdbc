organization := "com.rocketfuel.sdbc"

name := "jdbc"

libraryDependencies ++= Seq(
  //Connection pooling
  "com.zaxxer" % "HikariCP" % "2.5.1",
  "org.scodec" %% "scodec-bits" % "1.1.2",
  "com.chuusai" %% "shapeless" % "2.2.5",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

crossScalaVersions := Seq("2.10.5")
