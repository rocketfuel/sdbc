lazy val base = project.in(file("base"))

lazy val cassandra = project.in(file("cassandra")).dependsOn(base % "test->test;compile->compile")

lazy val jdbc = project.in(file("jdbc")).dependsOn(base % "test->test;compile->compile")

lazy val h2 = project.in(file("h2")).dependsOn(jdbc % "test->test;compile->compile")

lazy val h2Benchmarks = project.in(file("h2/benchmarks")).dependsOn(h2)

lazy val mysql = project.in(file("mariadb")).dependsOn(jdbc % "test->test;compile->compile")

lazy val postgresql = project.in(file("postgresql")).dependsOn(jdbc % "test->test;compile->compile")

lazy val sqlserver = project.in(file("sqlserver")).dependsOn(jdbc % "test->test;compile->compile")

lazy val examples = project.in(file("examples")).dependsOn(h2 % "test->test;compile->compile")

lazy val sdbc =
  project.in(file(".")).
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
      mysql,
      examples
    )

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.12")

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/rootdoc.txt")
