lazy val base = project.in(file("base"))

lazy val cassandra = project.in(file("cassandra")).dependsOn(base % "test->test;compile->compile")

lazy val cassandraScalaz = project.in(file("cassandra.scalaz")).dependsOn(cassandra % "test->test;compile->compile")

lazy val jdbc = project.in(file("jdbc")).dependsOn(base % "test->test;compile->compile")

lazy val h2 = project.in(file("h2")).dependsOn(jdbc % "test->test;compile->compile")

lazy val jdbcScalaz = project.in(file("jdbc.scalaz")).dependsOn(jdbc, h2 % "test->test")

lazy val jdbcPlay = project.in(file("jdbc.play")).dependsOn(jdbc % "test->test;compile->compile")

lazy val postgresql = project.in(file("postgresql")).dependsOn(jdbc % "test->test;compile->compile")

lazy val sqlserver = project.in(file("sqlserver")).dependsOn(jdbc % "test->test;compile->compile")

lazy val examples = project.in(file("examples")).dependsOn(h2 % "test->test;compile->compile")

lazy val root =
  project.
  in(file(".")).
  settings(publishArtifact := false).
  aggregate(
    base,
    cassandra,
    cassandraScalaz,
    jdbc,
    jdbcScalaz,
    h2,
    postgresql,
    sqlserver,
    examples
  )

organization in ThisBuild := "com.wda.sdbc"

scalaVersion in ThisBuild := "2.11.7"

crossScalaVersions := Seq("2.10.5")

version in ThisBuild := "0.11-SNAPSHOT"

licenses in ThisBuild := Seq("The BSD 3-Clause License" -> url("http://opensource.org/licenses/BSD-3-Clause"))

homepage in ThisBuild := Some(url("https://github.com/wdacom/"))

(publishArtifact in Test) in ThisBuild := true

pomExtra in ThisBuild :=
  <developers>
    <developer>
      <name>Jeff Shaw</name>
      <id>shawjef3</id>
      <url>https://github.com/shawjef3/</url>
      <organization>WDA</organization>
      <organizationUrl>http://www.wda.com/</organizationUrl>
    </developer>
  </developers>
  <scm>
    <url>git@github.com:wdacom/sdbc.git</url>
    <connection>scm:git:git@github.com:wdacom/sdbc.git</connection>
  </scm>

//Some helpful compiler flags from https://tpolecat.github.io/2014/04/11/scalac-flags.html
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",       // yes, this is 2 args
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Yno-adapted-args",
  "-Xfuture"
)
