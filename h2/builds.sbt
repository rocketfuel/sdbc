organization := "com.rocketfuel.sdbc"

name := "jdbc-h2"

description := "An implementation of SDBC for accessing H2 using JDBC."

libraryDependencies += "com.h2database" % "h2" % "1.4.190"

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
