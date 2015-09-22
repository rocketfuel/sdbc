organization := "com.rocketfuel.sdbc.h2"

name := "jdbc"

description := "An implementation of SDBC for accessing H2 using JDBC."

libraryDependencies += "com.h2database" % "h2" % "1.4.189"

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
