lazy val base = project.in(file("base"))

lazy val cassandra = project.in(file("cassandra")).dependsOn(base % "test->test;compile->compile")

lazy val jdbc = project.in(file("jdbc")).dependsOn(base % "test->test;compile->compile")

lazy val h2 = project.in(file("h2")).dependsOn(jdbc % "test->test;compile->compile")

lazy val h2Benchmarks = project.in(file("h2/benchmarks")).dependsOn(h2)

lazy val postgresql = project.in(file("postgresql")).dependsOn(jdbc % "test->test;compile->compile")

lazy val sqlserver = project.in(file("sqlserver")).dependsOn(jdbc % "test->test;compile->compile")

lazy val examples = project.in(file("examples")).dependsOn(h2 % "test->test;compile->compile")

lazy val sdbc =
  project.
  in(file(".")).
  settings(publishArtifact := false).
  settings(unidocSettings: _*).
  aggregate(
    base,
    cassandra,
    jdbc,
    h2,
    h2Benchmarks,
    postgresql,
    sqlserver,
    examples
  )

scalaVersion in ThisBuild := "2.11.8"

version in ThisBuild := "2.0-SNAPSHOT"

licenses in ThisBuild := Seq("The BSD 3-Clause License" -> url("http://opensource.org/licenses/BSD-3-Clause"))

homepage in ThisBuild := Some(url("https://github.com/wdacom/"))

(publishArtifact in Test) in ThisBuild := true

pomExtra in ThisBuild :=
  <developers>
    <developer>
      <name>Jeff Shaw</name>
      <id>shawjef3</id>
      <url>https://github.com/shawjef3/</url>
      <organization>Rocketfuel</organization>
      <organizationUrl>http://rocketfuel.com/</organizationUrl>
    </developer>
  </developers>
  <scm>
    <url>git@github.com:rocketfuel/sdbc.git</url>
    <connection>scm:git:git@github.com:rocketfuel/sdbc.git</connection>
  </scm>

//Some helpful compiler flags from https://tpolecat.github.io/2014/04/11/scalac-flags.html
scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",       // yes, this is 2 args
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-Yno-adapted-args",
  "-Xfuture"
)

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/rootdoc.txt")
