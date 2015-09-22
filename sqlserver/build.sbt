organization := "com.rocketfuel.sdbc.sqlserver"

name := "jdbc-java7"

description := "An implementation of WDA SDBC for accessing Microsoft SQL Server."

libraryDependencies += "net.sourceforge.jtds" % "jtds" % "1.3.1"

parallelExecution := false

crossScalaVersions := Seq("2.10.5")
