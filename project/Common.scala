import sbt._
import sbt.Keys._

object Common {

  val macroParadiseVersion = "2.1.0"

  val previousVersions = Seq()

  //Some helpful compiler flags from https://tpolecat.github.io/2014/04/11/scalac-flags.html
  def extraScalacOptions(scalaVersion: String) =
    Seq(
      "-deprecation",
      "-encoding", "UTF-8",       // yes, this is 2 args
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-unchecked"
    ) ++ {
      CrossVersion.partialVersion(scalaVersion) match {
        case Some((2, 11|12)) =>
          Seq("-Xfuture", "-Yno-adapted-args")
        case _ =>
          Seq.empty
      }
    }

  val settings = Seq(
    organization := "com.rocketfuel.sdbc",

    scalaVersion := "2.13.1",

    crossScalaVersions := Seq("2.11.12, 2.12.10"),

    version := "3.0.1",

    licenses := Seq("The BSD 3-Clause License" -> url("http://opensource.org/licenses/BSD-3-Clause")),

    homepage := Some(url("https://github.com/rocketfuel/sdbc")),

    pomExtra :=
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
        </scm>,

    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },

    publishMavenStyle := true,

    publishArtifact in Test := true,

    scalacOptions ++= extraScalacOptions(scalaVersion.value)
  )

  val xml = "org.scala-lang.modules" %% "scala-xml" % "1.2.0"

}
