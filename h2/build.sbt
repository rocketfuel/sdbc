organization := "com.rocketfuel.sdbc.h2"

name := "jdbc_java7"

description := "An implementation of WDA SDBC for accessing H2."

libraryDependencies += "com.h2database" % "h2" % "1.4.189"

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
