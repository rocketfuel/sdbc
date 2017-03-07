organization := "com.rocketfuel.sdbc"

name := "h2-jdbc"

description := "An implementation of SDBC for accessing H2 using JDBC."

libraryDependencies += "com.h2database" % "h2" % "1.4.193"

parallelExecution := false

Common.settings
