organization := "com.rocketfuel.sdbc"

name := "jdbc"

libraryDependencies ++= Seq(
  //Connection pooling
  "com.zaxxer" % "HikariCP" % "2.4.2",
  "org.scodec" %% "scodec-bits" % "1.0.11"
)

crossScalaVersions := Seq("2.10.5")
