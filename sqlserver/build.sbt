organization := "com.rocketfuel.sdbc.sqlserver"

name := "jdbc"

description := "An implementation of SDBC for accessing Microsoft SQL Server using JDBC."

libraryDependencies += "net.sourceforge.jtds" % "jtds" % "1.3.1"

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
