organization := "com.rocketfuel.sdbc.jdbc"

name := "base"

libraryDependencies ++= Seq(
  //Connection pooling
  "com.zaxxer" % "HikariCP" % "2.4.1",
  "org.scodec" %% "scodec-bits" % "1.0.10"
)

crossScalaVersions := Seq("2.10.5")
