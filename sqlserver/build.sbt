organization := "com.rocketfuel.sdbc"

name := "sqlserver-jdbc"

description := "An implementation of SDBC for accessing Microsoft SQL Server using JDBC."

libraryDependencies ++=
  Seq(
    "net.sourceforge.jtds" % "jtds" % "1.3.1",
    Common.xml
  )

parallelExecution := false

Common.settings
