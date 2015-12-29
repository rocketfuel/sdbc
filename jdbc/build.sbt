organization := "com.rocketfuel.sdbc"

name := "jdbc"

libraryDependencies ++= Seq(
  //Connection pooling
  "com.zaxxer" % "HikariCP" % "2.4.2",
  "org.scodec" %% "scodec-bits" % "1.0.11",
  "com.chuusai" %% "shapeless" % "2.2.5",
  "org.scalaz.stream" %% "scalaz-stream" % "0.8",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

crossScalaVersions := Seq("2.10.5")
